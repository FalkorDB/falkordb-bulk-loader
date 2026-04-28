import gzip
import re
import struct
import sys
from timeit import default_timer as timer

import click
import redis
from falkordb import FalkorDB
from pathos.pools import ThreadPool as Pool

from .entity_file import Type

BULK_EDGE_BATCH = 100_000
_PIPELINE_DEPTH = 5

# Valid Cypher identifier: letter/underscore, then alphanumeric/underscore
_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _validate_identifier(value, flag_name):
    if not _IDENTIFIER_RE.match(value):
        raise click.BadParameter(
            f"'{value}' is not a valid Cypher identifier (letters, digits, and underscores only, "
            "must start with a letter or underscore).",
            param_hint=f"'--{flag_name}'",
        )


def _open_mtx(path):
    if path.endswith(".gz"):
        return gzip.open(path, "rt", encoding="utf-8")
    return open(path, "rt", encoding="utf-8")


def _parse_header(first_line):
    """Parse the %%MatrixMarket header and return (field, symmetry)."""
    parts = first_line.strip().lower().split()
    if len(parts) < 5 or not parts[0].startswith("%%matrixmarket"):
        raise click.ClickException(
            "Not a valid MatrixMarket file (missing %%MatrixMarket header)."
        )
    if parts[1] != "matrix":
        raise click.ClickException(
            f"Unsupported object type '{parts[1]}' — only 'matrix' is supported."
        )
    if parts[2] != "coordinate":
        raise click.ClickException(
            f"Unsupported format '{parts[2]}' — only 'coordinate' (sparse) format is supported."
        )
    field = parts[3]
    symmetry = parts[4]
    if field not in ("real", "integer", "complex", "pattern"):
        raise click.ClickException(f"Unknown field type '{field}'.")
    if symmetry not in ("general", "symmetric", "skew-symmetric", "hermitian"):
        raise click.ClickException(f"Unknown symmetry type '{symmetry}'.")
    if symmetry in ("skew-symmetric", "hermitian"):
        raise click.ClickException(
            f"Symmetry type '{symmetry}' is not supported. "
            "Only 'general' and 'symmetric' are supported."
        )
    return field, symmetry


def _read_size_line(f):
    """Skip comment lines and return (nrows, ncols, nnz)."""
    for line in f:
        line = line.strip()
        if line and not line.startswith("%"):
            parts = line.split()
            if len(parts) != 3:
                raise click.ClickException(
                    f"Expected size line with 3 values, got: '{line}'."
                )
            return int(parts[0]), int(parts[1]), int(parts[2])
    raise click.ClickException("Unexpected end of file while reading matrix size.")


def _format_complex(real_str, imag_str):
    real = float(real_str)
    imag = float(imag_str)
    if imag >= 0:
        return f"{real}+{imag}i"
    return f"{real}{imag}i"


def _parse_entry(line, field):
    """Return (row, col, value) where value is None for pattern matrices."""
    parts = line.split()
    if field == "pattern":
        if len(parts) < 2:
            raise click.ClickException(f"Malformed entry line: '{line}'.")
        return int(parts[0]), int(parts[1]), None
    if field == "complex":
        if len(parts) < 4:
            raise click.ClickException(f"Malformed complex entry line: '{line}'.")
        return int(parts[0]), int(parts[1]), _format_complex(parts[2], parts[3])
    if field == "integer":
        if len(parts) < 3:
            raise click.ClickException(f"Malformed entry line: '{line}'.")
        return int(parts[0]), int(parts[1]), int(parts[2])
    # real
    if len(parts) < 3:
        raise click.ClickException(f"Malformed entry line: '{line}'.")
    return int(parts[0]), int(parts[1]), float(parts[2])


def _pack_reltype_header(rel_type, attr_name, field):
    """Build the GRAPH.BULK binary header for a relation type."""
    rel_enc = rel_type.encode()
    if attr_name:
        attr_enc = attr_name.encode()
        return struct.pack(
            f"={len(rel_enc) + 1}sI{len(attr_enc) + 1}s",
            rel_enc,
            1,
            attr_enc,
        )
    else:
        return struct.pack(f"={len(rel_enc) + 1}sI", rel_enc, 0)


def _pack_value(value, field):
    """Pack a single edge attribute value to binary."""
    if field == "real":
        return struct.pack("=Bd", Type.DOUBLE.value, value)
    elif field == "integer":
        return struct.pack("=Bq", Type.LONG.value, value)
    else:  # complex — already formatted as a string like "1.5+2.3i"
        enc = value.encode()
        return struct.pack(f"=B{len(enc) + 1}s", Type.STRING.value, enc)


def _bulk_send_edges(conn, graphname, rel_header, batch, field, use_attr):
    """Serialize one batch of edges and dispatch via GRAPH.BULK."""
    rows = bytearray()
    for e in batch:
        # MTX indices are 1-based; internal node IDs are 0-based.
        rows += struct.pack("=QQ", e[0] - 1, e[1] - 1)
        if use_attr:
            rows += _pack_value(e[2], field)
    conn.execute_command(
        "GRAPH.BULK",
        graphname,
        0,
        len(batch),
        0,
        1,
        rel_header + bytes(rows),
    )


################################################################################
# MTX bulk loader
################################################################################
@click.command()
@click.argument("graph")
@click.argument("mtx_file")
@click.option(
    "--server-url",
    "-u",
    default="redis://127.0.0.1:6379",
    help="FalkorDB connection url (default: redis://127.0.0.1:6379)",
)
@click.option(
    "--node-label",
    "-l",
    default="Node",
    help="Label to assign to nodes (default: Node)",
)
@click.option(
    "--relation-type",
    "-t",
    default="CONNECTS",
    help="Relation type for edges (default: CONNECTS)",
)
@click.option(
    "--attr-name",
    "-a",
    default=None,
    help="Property name to store edge values on (optional; values are discarded if not set)",
)
def mtx_insert(graph, mtx_file, server_url, node_label, relation_type, attr_name):
    """Load a Matrix Market (.mtx) file into a FalkorDB graph.

    GRAPH is the name of the graph to create.
    MTX_FILE is the path to the .mtx (or .mtx.gz) file.
    """
    if sys.version_info < (3, 10):
        raise RuntimeError("Python >= 3.10 is required for the falkordb MTX loader.")

    _validate_identifier(node_label, "node-label")
    _validate_identifier(relation_type, "relation-type")
    if attr_name is not None:
        _validate_identifier(attr_name, "attr-name")

    start_time = timer()

    client = FalkorDB.from_url(server_url)

    try:
        client.connection.ping()
    except redis.exceptions.ConnectionError as e:
        print("Could not connect to FalkorDB server.")
        raise e

    try:
        module_list = [m["name"] for m in client.connection.module_list()]
        if "graph" not in module_list:
            print("FalkorDB module not loaded on connected server.")
            sys.exit(1)
    except redis.exceptions.ResponseError:
        pass

    if client.connection.exists(graph):
        raise click.ClickException(
            f"Graph '{graph}' already exists. Choose a different name or delete the existing graph."
        )

    # Parse the header and size line before touching the database
    with _open_mtx(mtx_file) as f:
        first_line = f.readline()
        field, symmetry = _parse_header(first_line)
        nrows, ncols, nnz = _read_size_line(f)

    if nrows != ncols:
        raise click.ClickException(
            f"Non-square matrices are not supported ({nrows}×{ncols}). "
            "Only square matrices can be loaded."
        )

    is_symmetric = symmetry == "symmetric"
    is_valued = field != "pattern"
    use_attr = attr_name is not None and is_valued

    g = client.select_graph(graph)

    # Phase 1: Create all N nodes via Cypher — fast single query, assigns
    # internal IDs 0..N-1 sequentially (matrix index i → internal ID i-1).
    print(f"Creating {nrows} nodes...")
    g.query(
        f"UNWIND range(1, $n) AS id CREATE (:{node_label} {{id: id}})",
        params={"n": nrows},
    )

    # Phase 2: Stream entries and dispatch edge batches via GRAPH.BULK.
    # Internal node IDs are known from the sequential creation above — no
    # index or MATCH needed.
    print(f"Loading {nnz} stored entries...")
    rel_header = _pack_reltype_header(
        relation_type, attr_name if use_attr else None, field
    )
    conn = client.connection

    pool = Pool(nodes=1)
    tasks = []

    def _submit(batch):
        task = pool.apipe(
            _bulk_send_edges, conn, graph, rel_header, batch, field, use_attr
        )
        tasks.append(task)
        if len(tasks) >= _PIPELINE_DEPTH:
            tasks.pop(0).get()

    edge_batch = []
    entries_read = 0
    edges_created = 0

    with _open_mtx(mtx_file) as f:
        f.readline()  # skip %%MatrixMarket header
        _read_size_line(f)  # skip comments + size line

        with click.progressbar(length=nnz, label="Loading edges") as bar:
            for line in f:
                line = line.strip()
                if not line or line.startswith("%"):
                    continue

                row, col, value = _parse_entry(line, field)

                if row < 1 or row > nrows or col < 1 or col > ncols:
                    raise click.ClickException(
                        f"Entry ({row}, {col}) is out of bounds for a {nrows}×{ncols} matrix."
                    )

                if use_attr:
                    edge_batch.append((row, col, value))
                    if is_symmetric and row != col:
                        edge_batch.append((col, row, value))
                else:
                    edge_batch.append((row, col))
                    if is_symmetric and row != col:
                        edge_batch.append((col, row))

                entries_read += 1
                edges_created += 2 if (is_symmetric and row != col) else 1
                bar.update(1)

                if len(edge_batch) >= BULK_EDGE_BATCH:
                    _submit(edge_batch)
                    edge_batch = []

        if edge_batch:
            _submit(edge_batch)

    for t in tasks:
        t.get()

    if entries_read != nnz:
        print(
            f"Warning: expected {nnz} entries but read {entries_read}. "
            "The file may be truncated or malformed."
        )

    end_time = timer()
    print(
        f"Graph '{graph}' created: {nrows} nodes, {edges_created} edges "
        f"({entries_read} stored entries) in {end_time - start_time:.2f} seconds."
    )


if __name__ == "__main__":
    mtx_insert()
