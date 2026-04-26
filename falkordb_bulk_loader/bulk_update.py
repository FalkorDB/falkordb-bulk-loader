import ast
import csv
import json
import math
import re
import sys
from timeit import default_timer as timer

import click
import redis
from falkordb import FalkorDB


def utf8len(s):
    return len(s.encode("utf-8"))


# Match Cypher-style boolean/null tokens (lowercase ``true``/``false``/``null``)
# as standalone words, so we can substitute them with their Python equivalents
# before handing the cell to ``ast.literal_eval``.  The lookarounds prevent us
# from rewriting tokens that appear inside string literals (e.g. ``'truthy'``)
# or as part of longer identifiers.
_CYPHER_LITERAL_RE = re.compile(r"(?<![A-Za-z0-9_'\"])(true|false|null)(?![A-Za-z0-9_'\"])")
_CYPHER_TO_PYTHON = {"true": "True", "false": "False", "null": "None"}


# Count number of rows in file.
def count_entities(filename):
    entities_count = 0
    with open(filename, "rt") as f:
        entities_count = sum(1 for line in f)
    return entities_count


def convert_cell(cell):
    """Convert a CSV cell string to the most appropriate Python scalar.

    Conversion order: int -> float -> bool -> list -> str.
    Empty / whitespace-only cells are returned as an empty string so that
    existing Cypher guards such as ``row[i] <> ''`` continue to work.
    Array-literal cells (e.g. ``[1,'nested_str']``) are parsed into Python
    lists so that FalkorDB stores them as array properties, preserving the
    behaviour of the original bulk updater.  Both Python literal syntax
    (``[True, False, None]``) and Cypher literal syntax
    (``[true, false, null]``) are accepted; the lowercase Cypher keywords are
    rewritten to their Python equivalents before parsing.
    """
    cell = cell.strip()
    if cell == "":
        return ""
    try:
        return int(cell)
    except ValueError:
        pass
    try:
        val = float(cell)
        if math.isnan(val) or math.isinf(val):
            return cell  # keep "NaN"/"Infinity"/etc. as a string
        return val
    except ValueError:
        pass
    if cell.lower() == "true":
        return True
    if cell.lower() == "false":
        return False
    if cell.startswith("[") and cell.endswith("]"):
        # Try Python literal syntax first ([1, 'a', True, None]).
        try:
            parsed = ast.literal_eval(cell)
            if isinstance(parsed, list):
                return parsed
        except (ValueError, SyntaxError):
            pass
        # Fall back to Cypher-style literals ([true, false, null]) by rewriting
        # the lowercase keywords to their Python equivalents and retrying.  The
        # regex skips tokens that appear inside string literals.
        if _CYPHER_LITERAL_RE.search(cell):
            rewritten = _CYPHER_LITERAL_RE.sub(
                lambda m: _CYPHER_TO_PYTHON[m.group(1)], cell
            )
            try:
                parsed = ast.literal_eval(rewritten)
                if isinstance(parsed, list):
                    return parsed
            except (ValueError, SyntaxError):
                pass
    return cell


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
        result = self.graph.query(self.query, params={"rows": rows})
        self.update_statistics(result)

    # Raise an exception if the query triggers a compile-time error
    def validate_query(self):
        # The explain call will raise an error if the query is malformed or invalid.
        self.graph.explain(self.query, params={"rows": []})

    def process_update_csv(self):
        entity_count = count_entities(self.filename)

        with open(self.filename, "rt") as f:
            if self.no_header is False:
                next(f)  # skip header

            reader = csv.reader(
                f,
                delimiter=self.separator,
                skipinitialspace=True,
            )

            rows = []
            with click.progressbar(
                reader, length=entity_count, label=self.graph_name
            ) as reader:
                for row in reader:
                    # Convert each cell to the appropriate Python type.
                    converted = [convert_cell(cell) for cell in row]

                    # Measure the serialised byte size of this row using json.dumps,
                    # which is a conservative proxy for the FalkorDB client's
                    # stringify_param_value encoding (same quoting/escaping for strings,
                    # same numeric formatting, same null/bool literals).  This prevents
                    # batches from exceeding Redis's proto-max-bulk-len limit.
                    # +1 accounts for the separator comma between rows in the list.
                    added_size = utf8len(json.dumps(converted, ensure_ascii=False)) + 1

                    # A single row larger than the configured budget cannot be
                    # batched safely.  Raising here avoids emitting an empty
                    # batch followed by an oversized one that the server would
                    # reject with a cryptic proto-max-bulk-len error.
                    if added_size > self.max_token_size:
                        raise ValueError(
                            f"Row exceeds max token size "
                            f"({added_size} bytes > {self.max_token_size} bytes "
                            f"after subtracting the query envelope). "
                            f"Increase --max-token-size or split the row."
                        )

                    # Emit the current buffer if the max token size would be exceeded.
                    if self.buffer_size + added_size > self.max_token_size:
                        self.emit_buffer(rows)
                        rows = []
                        self.buffer_size = 0

                    rows.append(converted)
                    self.buffer_size += added_size

            self.emit_buffer(rows)


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
# Cypher query options
@click.option("--query", "-q", help="Query to run on server")
@click.option(
    "--variable-name",
    "-v",
    default="row",
    help="Variable name for row array in queries (default: row)",
)
# CSV file options
@click.option("--csv", "-c", help="Path to CSV input file")
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
def bulk_update(
    graph,
    server_url,
    query,
    variable_name,
    csv,
    separator,
    no_header,
    max_token_size,
):
    if sys.version_info[0] < 3:
        raise Exception("Python 3 is required for the falkordb bulk updater.")

    start_time = timer()

    # Attempt to connect to the server
    client = FalkorDB.from_url(server_url)
    try:
        client.connection.ping()
    except redis.exceptions.ConnectionError as e:
        print("Could not connect to server.")
        raise e

    # Attempt to verify that falkordb module is loaded
    try:
        module_list = [m["name"] for m in client.connection.module_list()]
        if "graph" not in module_list:
            print("FalkorDB module not loaded on connected server.")
            sys.exit(1)
    except redis.exceptions.ResponseError:
        # Ignore check if the connected server does not support the "MODULE LIST" command
        pass

    updater = BulkUpdate(
        graph, max_token_size, separator, no_header, csv, query, variable_name, client
    )

    if graph in client.list_graphs():
        updater.validate_query()
    else:
        g = client.select_graph(graph)
        # create the graph
        g.query("RETURN 1")
        updater.validate_query()
        g.delete()

    updater.process_update_csv()

    end_time = timer()

    for key, value in updater.statistics.items():
        print(key + ": " + repr(value))
    print(f"Update of graph '{graph}' complete in {end_time - start_time:f} seconds")


if __name__ == "__main__":
    bulk_update()
