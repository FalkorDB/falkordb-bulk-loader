import csv
import logging
import os
import sys
from timeit import default_timer as timer

import click
import redis
from falkordb import FalkorDB

from .stacktrace import register_stacktrace_dump_handler

logger = logging.getLogger(__name__)


def utf8len(s):
    return len(s.encode("utf-8"))


def parse_query_file(query_file):
    query_map = {}
    with open(query_file, "rt") as f:
        reader = csv.reader(f)
        for line_num, row in enumerate(reader, start=1):
            if len(row) < 2:
                raise click.ClickException(
                    f"{query_file}:{line_num} expected 2 columns: input file, query"
                )

            input_file = row[0].strip()
            query = ",".join(row[1:]).strip()
            if input_file == "" or query == "":
                raise click.ClickException(
                    f"{query_file}:{line_num} expected non-empty input file and query"
                )

            if input_file in query_map:
                raise click.ClickException(
                    f"{query_file}:{line_num} duplicate query mapping for '{input_file}'"
                )
            query_map[input_file] = query

    if len(query_map) == 0:
        raise click.ClickException(f"{query_file} did not include any query mappings")
    return query_map


def query_for_input_file(input_file, query_map):
    # Allow mappings by original argument, absolute path, or basename.
    candidates = [input_file, os.path.abspath(input_file), os.path.basename(input_file)]
    queries = [
        query_map[candidate] for candidate in candidates if candidate in query_map
    ]

    if len(queries) == 0:
        raise click.ClickException(
            f"No query mapping found for input file '{input_file}'. "
            "Add a matching row in --query-file."
        )

    if len(set(queries)) > 1:
        raise click.ClickException(
            f"Ambiguous query mapping for '{input_file}'. "
            "Use a single unambiguous key in --query-file."
        )
    return queries[0]


def collect_jobs(csv_file, query, nodes, relations, query_file):
    using_single_file_mode = bool(csv_file or query)
    using_multi_file_mode = bool(any(nodes) or any(relations) or query_file)

    if using_single_file_mode and using_multi_file_mode:
        raise click.ClickException(
            "Do not combine --csv/--query with --nodes/--relations/--query-file."
        )

    if using_multi_file_mode:
        if not query_file:
            raise click.ClickException(
                "--query-file is required when using --nodes/--relations."
            )
        if not (any(nodes) or any(relations)):
            raise click.ClickException(
                "At least one input file is required via --nodes or --relations."
            )

        query_map = parse_query_file(query_file)
        jobs = []

        # Enforce node files first, then relation files.
        for node_file in nodes:
            jobs.append((node_file, query_for_input_file(node_file, query_map)))
        for relation_file in relations:
            jobs.append((relation_file, query_for_input_file(relation_file, query_map)))
        return jobs

    if not csv_file or not query:
        raise click.ClickException("Single-file mode requires both --csv and --query.")
    return [(csv_file, query)]


# Count number of rows in file.
def count_entities(filename):
    entities_count = 0
    with open(filename, "rt") as f:
        entities_count = sum(1 for line in f)
    return entities_count


class BulkUpdate:
    """Handler class for emitting bulk update commands"""

    def __init__(
        self,
        graph_name,
        max_token_size,
        separator,
        no_header,
        filename,
        query,
        variable_name,
        client,
    ):
        self.separator = separator
        self.no_header = no_header
        self.query = " ".join(["UNWIND $rows AS", variable_name, query])
        self.buffer_size = 0
        self.max_token_size = max_token_size * 1024 * 1024 - utf8len(self.query)
        self.filename = filename
        self.graph_name = graph_name
        self.graph = client.select_graph(graph_name)
        self.statistics = {}
        self.buffers_sent = 0

    def update_statistics(self, result):
        self.update_statistic("Nodes created", result.nodes_created)
        self.update_statistic("Labels added", result.labels_added)
        self.update_statistic("Relationships created", result.relationships_created)
        self.update_statistic("Properties set", result.properties_set)

    def update_statistic(self, key, new_val):
        if new_val == 0:
            return

        try:
            val = self.statistics[key]
        except KeyError:
            val = 0
        val += int(new_val)
        self.statistics[key] = val

    def emit_buffer(self, rows):
        command = " ".join([rows, self.query])
        self.buffers_sent += 1
        logger.debug(
            f"Sending buffer #{self.buffers_sent} "
            f"({utf8len(command)} bytes) to FalkorDB..."
        )
        result = self.graph.query(command)
        self.update_statistics(result)

    def quote_string(self, cell):
        cell = cell.strip()
        # Quote-interpolate cell if it is an unquoted string.
        try:
            float(cell)  # Check for numeric
        except ValueError:
            if (
                (cell.lower() != "false" and cell.lower() != "true")
                and (cell[0] != "[" and cell.lower != "]")  # Check for boolean
                and (cell[0] != '"' and cell[-1] != '"')  # Check for array
                and (  # Check for double-quoted string
                    cell[0] != "'" and cell[-1] != "'"
                )
            ):  # Check for single-quoted string
                cell = "".join(['"', cell, '"'])
        return cell

    # Raise an exception if the query triggers a compile-time error
    def validate_query(self):
        command = " ".join(["CYPHER rows=[]", self.query])
        # The plan call will raise an error if the query is malformed or invalid.
        self.graph.explain(command)

    def process_update_csv(self):
        entity_count = count_entities(self.filename)

        with open(self.filename, "rt") as f:
            if self.no_header is False:
                next(f)  # skip header

            reader = csv.reader(
                f,
                delimiter=self.separator,
                skipinitialspace=True,
                quoting=csv.QUOTE_NONE,
                escapechar="\\",
            )

            rows_strs = []
            with click.progressbar(
                reader, length=entity_count, label=self.graph_name
            ) as reader:
                for row in reader:
                    # Prepare the string representation of the current row.
                    row = ",".join([self.quote_string(cell) for cell in row])
                    next_line = "".join(["[", row.strip(), "]"])

                    # Emit buffer now if the max token size would be exceeded by this addition.
                    added_size = (
                        utf8len(next_line) + 1
                    )  # Add one to compensate for the added comma.
                    if self.buffer_size + added_size > self.max_token_size:
                        # Concatenate all rows into a valid parameter set
                        buf = "".join(["CYPHER rows=[", ",".join(rows_strs), "]"])
                        self.emit_buffer(buf)
                        rows_strs = []
                        self.buffer_size = 0

                    # Concatenate the string into the rows string representation.
                    rows_strs.append(next_line)
                    self.buffer_size += added_size
            # Concatenate all rows into a valid parameter set
            buf = "".join(["CYPHER rows=[", ",".join(rows_strs), "]"])
            self.emit_buffer(buf)


################################################################################
# Bulk updater
################################################################################
# Command-line arguments
@click.command()
@click.argument("graph")
# Server connection settings
@click.option(
    "--server-url",
    "-u",
    default="falkor://127.0.0.1:6379",
    help="FalkorDB connection url",
)
@click.option(
    "--socket-timeout",
    type=click.FloatRange(min=0.0, min_open=True),
    default=None,
    help="Socket read/write timeout in seconds (forwarded to FalkorDB client).",
)
@click.option(
    "--socket-connect-timeout",
    type=click.FloatRange(min=0.0, min_open=True),
    default=None,
    help="Socket connection timeout in seconds (forwarded to FalkorDB client).",
)
# Cypher query options
@click.option("--query", "-q", help="Query to run on server")
@click.option(
    "--query-file",
    help="Path to CSV mapping input files to per-file queries. "
    "Each row must be: input_file,query",
)
@click.option(
    "--variable-name",
    "-v",
    default="row",
    help="Variable name for row array in queries (default: row)",
)
# CSV file options
@click.option("--csv", "-c", "csv_file", help="Path to CSV input file")
@click.option(
    "--nodes",
    multiple=True,
    help="Path to node input file (processed before relations when using --query-file)",
)
@click.option(
    "--relations",
    multiple=True,
    help="Path to relation input file (processed after nodes when using --query-file)",
)
@click.option(
    "--separator", "-o", default=",", help="Field token separator in CSV file"
)
@click.option(
    "--no-header",
    "-n",
    default=False,
    is_flag=True,
    help="If set, the CSV file has no header",
)
# Buffer size restrictions
@click.option(
    "--max-token-size",
    "-t",
    default=500,
    help="Max size of each token in megabytes (default 500, max 512)",
)
@click.option(
    "--verbose",
    default=False,
    is_flag=True,
    help="Print extra information about the steps performed during the update",
)
def bulk_update(
    graph,
    server_url,
    socket_timeout,
    socket_connect_timeout,
    query,
    query_file,
    variable_name,
    csv_file,
    nodes,
    relations,
    separator,
    no_header,
    max_token_size,
    verbose,
):
    if sys.version_info < (3, 10):
        raise RuntimeError("Python >= 3.10 is required for the falkordb bulk updater.")

    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        stream=sys.stdout,
        force=True,
    )

    # Allow operators to dump stack traces of all threads via `kill -SIGUSR1 <pid>`.
    register_stacktrace_dump_handler()

    start_time = timer()

    # Attempt to connect to the server
    logger.debug(f"Connecting to FalkorDB server at '{server_url}'...")
    client = FalkorDB.from_url(
        server_url,
        socket_timeout=socket_timeout,
        socket_connect_timeout=socket_connect_timeout,
    )
    try:
        client.connection.ping()
    except redis.exceptions.ConnectionError as e:
        logger.error("Could not connect to server.")
        raise e

    logger.debug("Connected to FalkorDB server.")

    # Attempt to verify that falkordb module is loaded
    try:
        module_list = [m["name"] for m in client.connection.module_list()]
        if "graph" not in module_list:
            logger.error("FalkorDB module not loaded on connected server.")
            sys.exit(1)
        logger.debug("FalkorDB module is loaded on the server.")
    except redis.exceptions.ResponseError:
        # Ignore check if the connected server does not support the "MODULE LIST" command
        logger.debug(
            "Server does not support 'MODULE LIST'; skipping FalkorDB module check."
        )

    jobs = collect_jobs(csv_file, query, nodes, relations, query_file)

    logger.debug(f"Validating query against graph '{graph}'...")

    if graph in client.list_graphs():
        for filename, file_query in jobs:
            updater = BulkUpdate(
                graph,
                max_token_size,
                separator,
                no_header,
                filename,
                file_query,
                variable_name,
                client,
            )
            updater.validate_query()
    else:
        g = client.select_graph(graph)
        # create the graph
        g.query("RETURN 1")
        for filename, file_query in jobs:
            updater = BulkUpdate(
                graph,
                max_token_size,
                separator,
                no_header,
                filename,
                file_query,
                variable_name,
                client,
            )
            updater.validate_query()
        g.delete()

    statistics = {}
    for filename, file_query in jobs:
        logger.debug(f"Processing CSV file '{filename}'...")
        updater = BulkUpdate(
            graph,
            max_token_size,
            separator,
            no_header,
            filename,
            file_query,
            variable_name,
            client,
        )
        updater.process_update_csv()
        for key, value in updater.statistics.items():
            statistics[key] = statistics.get(key, 0) + value

    end_time = timer()

    for key, value in statistics.items():
        logger.info(key + ": " + repr(value))
    logger.info(
        f"Update of graph '{graph}' complete in {end_time - start_time:f} seconds"
    )


if __name__ == "__main__":
    bulk_update()
