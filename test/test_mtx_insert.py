import gzip
import os
import tempfile

from click.testing import CliRunner
from falkordb import FalkorDB

from falkordb_bulk_loader.mtx_insert import mtx_insert

EXAMPLE_DIR = os.path.join(os.path.dirname(__file__), "..", "example")


def write_mtx(path, content):
    with open(path, "w") as f:
        f.write(content)


def write_mtx_gz(path, content):
    with gzip.open(path, "wt", encoding="utf-8") as f:
        f.write(content)


class TestMtxInsert:
    db_con = FalkorDB(host="localhost", port=6379)

    @classmethod
    def setup_class(cls):
        cls.db_con.flushdb()
        cls.runner = CliRunner()
        cls.tmp_files = []

    @classmethod
    def teardown_class(cls):
        for f in cls.tmp_files:
            if os.path.isfile(f):
                os.unlink(f)
        cls.db_con.flushdb()

    def _tmpfile(self, suffix=".mtx"):
        fd, path = tempfile.mkstemp(suffix=suffix)
        os.close(fd)
        self.tmp_files.append(path)
        return path

    # ------------------------------------------------------------------
    # Helper: invoke the CLI and return the result
    # ------------------------------------------------------------------
    def _invoke(self, graph, mtx_path, extra_args=None):
        args = [graph, mtx_path]
        if extra_args:
            args += extra_args
        return self.runner.invoke(mtx_insert, args, catch_exceptions=False)

    # ------------------------------------------------------------------
    # Basic pattern matrix (no values)
    # ------------------------------------------------------------------
    def test_pattern_general(self):
        """Pattern general matrix creates correct nodes and edges."""
        path = self._tmpfile()
        write_mtx(
            path,
            "%%MatrixMarket matrix coordinate pattern general\n"
            "% a comment\n"
            "4 4 3\n"
            "1 2\n"
            "2 3\n"
            "3 4\n",
        )
        res = self._invoke("mtx_pattern_general", path)
        assert res.exit_code == 0, res.output

        g = self.db_con.select_graph("mtx_pattern_general")
        assert g.query("MATCH (n) RETURN count(n)").result_set[0][0] == 4
        assert g.query("MATCH ()-[r]->() RETURN count(r)").result_set[0][0] == 3

    # ------------------------------------------------------------------
    # Real-valued matrix with attr-name
    # ------------------------------------------------------------------
    def test_real_with_attr(self):
        """Real-valued matrix stores edge property when --attr-name is given."""
        path = self._tmpfile()
        write_mtx(
            path,
            "%%MatrixMarket matrix coordinate real general\n3 3 2\n1 2 1.5\n2 3 2.5\n",
        )
        res = self._invoke("mtx_real_attr", path, ["--attr-name", "weight"])
        assert res.exit_code == 0, res.output

        g = self.db_con.select_graph("mtx_real_attr")
        weights = sorted(
            r[0]
            for r in g.query("MATCH ()-[r:CONNECTS]->() RETURN r.weight").result_set
        )
        assert weights == [1.5, 2.5]

    # ------------------------------------------------------------------
    # Real-valued matrix without attr-name — values are discarded
    # ------------------------------------------------------------------
    def test_real_no_attr(self):
        """Values are silently discarded when --attr-name is not given."""
        path = self._tmpfile()
        write_mtx(
            path,
            "%%MatrixMarket matrix coordinate real general\n"
            "3 3 2\n"
            "1 2 99.0\n"
            "2 3 100.0\n",
        )
        res = self._invoke("mtx_real_no_attr", path)
        assert res.exit_code == 0, res.output

        g = self.db_con.select_graph("mtx_real_no_attr")
        assert g.query("MATCH ()-[r]->() RETURN count(r)").result_set[0][0] == 2
        # No weight property
        props = g.query("MATCH ()-[r]->() RETURN r.weight").result_set
        assert all(row[0] is None for row in props)

    # ------------------------------------------------------------------
    # Symmetric matrix — reverse edges are created for off-diagonal entries
    # ------------------------------------------------------------------
    def test_symmetric_edges(self):
        """Symmetric matrix creates both (i,j) and (j,i) for off-diagonal entries."""
        path = self._tmpfile()
        write_mtx(
            path,
            "%%MatrixMarket matrix coordinate pattern symmetric\n"
            "4 4 3\n"
            "2 1\n"  # off-diagonal → 2 edges
            "3 1\n"  # off-diagonal → 2 edges
            "1 1\n",  # diagonal → 1 self-loop
        )
        res = self._invoke("mtx_symmetric", path)
        assert res.exit_code == 0, res.output

        g = self.db_con.select_graph("mtx_symmetric")
        assert g.query("MATCH ()-[r]->() RETURN count(r)").result_set[0][0] == 5

    # ------------------------------------------------------------------
    # Integer-valued matrix
    # ------------------------------------------------------------------
    def test_integer_values(self):
        """Integer-valued entries are stored as integers."""
        path = self._tmpfile()
        write_mtx(
            path,
            "%%MatrixMarket matrix coordinate integer general\n3 3 1\n1 3 42\n",
        )
        res = self._invoke("mtx_integer", path, ["--attr-name", "val"])
        assert res.exit_code == 0, res.output

        g = self.db_con.select_graph("mtx_integer")
        val = g.query("MATCH ()-[r]->() RETURN r.val").result_set[0][0]
        assert val == 42

    # ------------------------------------------------------------------
    # Complex-valued matrix — stored as strings
    # ------------------------------------------------------------------
    def test_complex_values(self):
        """Complex entries are stored as 'real+imagi' strings."""
        path = self._tmpfile()
        write_mtx(
            path,
            "%%MatrixMarket matrix coordinate complex general\n"
            "3 3 2\n"
            "1 2 1.0 2.0\n"
            "2 3 3.0 -1.0\n",
        )
        res = self._invoke("mtx_complex", path, ["--attr-name", "z"])
        assert res.exit_code == 0, res.output

        g = self.db_con.select_graph("mtx_complex")
        values = {r[0] for r in g.query("MATCH ()-[r]->() RETURN r.z").result_set}
        assert "1.0+2.0i" in values
        assert "3.0-1.0i" in values

    # ------------------------------------------------------------------
    # Custom label and relation type
    # ------------------------------------------------------------------
    def test_custom_label_and_type(self):
        """Custom --node-label and --relation-type are applied."""
        path = self._tmpfile()
        write_mtx(
            path,
            "%%MatrixMarket matrix coordinate pattern general\n3 3 1\n1 2\n",
        )
        res = self._invoke(
            "mtx_custom_labels",
            path,
            ["--node-label", "Vertex", "--relation-type", "EDGE"],
        )
        assert res.exit_code == 0, res.output

        g = self.db_con.select_graph("mtx_custom_labels")
        assert g.query("MATCH (n:Vertex) RETURN count(n)").result_set[0][0] == 3
        assert g.query("MATCH ()-[r:EDGE]->() RETURN count(r)").result_set[0][0] == 1

    # ------------------------------------------------------------------
    # Gzipped file
    # ------------------------------------------------------------------
    def test_gzip_file(self):
        """Gzipped .mtx.gz files are loaded correctly."""
        path = self._tmpfile(suffix=".mtx.gz")
        write_mtx_gz(
            path,
            "%%MatrixMarket matrix coordinate pattern general\n3 3 2\n1 2\n2 3\n",
        )
        res = self._invoke("mtx_gzip", path)
        assert res.exit_code == 0, res.output

        g = self.db_con.select_graph("mtx_gzip")
        assert g.query("MATCH (n) RETURN count(n)").result_set[0][0] == 3
        assert g.query("MATCH ()-[r]->() RETURN count(r)").result_set[0][0] == 2

    # ------------------------------------------------------------------
    # Error: graph already exists
    # ------------------------------------------------------------------
    def test_refuses_existing_graph(self):
        """CLI refuses to overwrite an existing graph."""
        path = self._tmpfile()
        write_mtx(
            path,
            "%%MatrixMarket matrix coordinate pattern general\n2 2 1\n1 2\n",
        )
        # Create it once
        res = self._invoke("mtx_existing", path)
        assert res.exit_code == 0, res.output

        # Attempt to create again — should fail
        res2 = self.runner.invoke(mtx_insert, ["mtx_existing", path])
        assert res2.exit_code != 0
        assert "already exists" in res2.output

    # ------------------------------------------------------------------
    # Error: non-square matrix
    # ------------------------------------------------------------------
    def test_refuses_non_square(self):
        """Non-square matrices are rejected with a clear error."""
        path = self._tmpfile()
        write_mtx(
            path,
            "%%MatrixMarket matrix coordinate pattern general\n3 5 1\n1 2\n",
        )
        res = self.runner.invoke(mtx_insert, ["mtx_nonsquare", path])
        assert res.exit_code != 0
        assert "Non-square" in res.output

    # ------------------------------------------------------------------
    # Error: unsupported symmetry type
    # ------------------------------------------------------------------
    def test_refuses_hermitian(self):
        path = self._tmpfile()
        write_mtx(
            path,
            "%%MatrixMarket matrix coordinate complex hermitian\n2 2 1\n2 1 1.0 0.5\n",
        )
        res = self.runner.invoke(mtx_insert, ["mtx_hermitian", path])
        assert res.exit_code != 0
        assert "hermitian" in res.output.lower()

    # ------------------------------------------------------------------
    # Error: array format
    # ------------------------------------------------------------------
    def test_refuses_array_format(self):
        path = self._tmpfile()
        write_mtx(
            path,
            "%%MatrixMarket matrix array real general\n2 2\n1.0\n2.0\n3.0\n4.0\n",
        )
        res = self.runner.invoke(mtx_insert, ["mtx_array", path])
        assert res.exit_code != 0
        assert "coordinate" in res.output.lower()

    # ------------------------------------------------------------------
    # Error: invalid identifier
    # ------------------------------------------------------------------
    def test_invalid_node_label(self):
        path = self._tmpfile()
        write_mtx(
            path,
            "%%MatrixMarket matrix coordinate pattern general\n2 2 1\n1 2\n",
        )
        res = self.runner.invoke(
            mtx_insert, ["mtx_badlabel", path, "--node-label", "bad-label"]
        )
        assert res.exit_code != 0
        assert "valid Cypher identifier" in res.output

    # ------------------------------------------------------------------
    # Node IDs are 1-based and stored as integers
    # ------------------------------------------------------------------
    def test_node_ids_are_one_based(self):
        """Nodes have integer id properties starting at 1."""
        path = self._tmpfile()
        write_mtx(
            path,
            "%%MatrixMarket matrix coordinate pattern general\n3 3 0\n",
        )
        res = self._invoke("mtx_node_ids", path)
        assert res.exit_code == 0, res.output

        g = self.db_con.select_graph("mtx_node_ids")
        ids = sorted(r[0] for r in g.query("MATCH (n:Node) RETURN n.id").result_set)
        assert ids == [1, 2, 3]

    # ------------------------------------------------------------------
    # Real example: p2p-Gnutella04 (10879 nodes, 39994 edges)
    # ------------------------------------------------------------------
    def test_p2p_gnutella04(self):
        """Load the bundled p2p-Gnutella04.mtx example and verify node/edge counts."""
        mtx_path = os.path.join(EXAMPLE_DIR, "p2p-Gnutella04.mtx")
        res = self._invoke("mtx_p2p_gnutella04", mtx_path)
        assert res.exit_code == 0, res.output

        g = self.db_con.select_graph("mtx_p2p_gnutella04")
        node_count = g.query("MATCH (n) RETURN count(n)").result_set[0][0]
        edge_count = g.query("MATCH ()-[r]->() RETURN count(r)").result_set[0][0]
        assert node_count == 10879
        assert edge_count == 39994
