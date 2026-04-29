import logging
import sys
from timeit import default_timer as timer

import click
import redis
from falkordb import FalkorDB

from .config import Config
from .exceptions import CSVError
from .label import Label
from .query_buffer import QueryBuffer
from .relation_type import RelationType
from .stacktrace import register_stacktrace_dump_handler

logger = logging.getLogger(__name__)


def parse_schemas(cls, query_buf, path_to_csv, csv_tuples, config):
    schemas = [None] * (len(path_to_csv) + len(csv_tuples))
    for idx, in_csv in enumerate(path_to_csv):
        # Build entity descriptor from input CSV
        schemas[idx] = cls(query_buf, in_csv, None, config)

    offset = len(path_to_csv)
    for idx, csv_tuple in enumerate(csv_tuples):
        # Build entity descriptor from input CSV
        schemas[idx + offset] = cls(query_buf, csv_tuple[1], csv_tuple[0], config)
    return schemas


# For each input file, validate contents and convert to binary format.
# If any buffer limits have been reached, flush all enqueued inserts to Redis.
def process_entities(entities):
    for entity in entities:
        entity.process_entities()
        added_size = entity.binary_size
        # Check to see if the addition of this data will exceed the buffer's capacity
        if (
            entity.query_buffer.buffer_size + added_size
            >= entity.config.max_buffer_size
            or entity.query_buffer.redis_token_count + len(entity.binary_entities)
            >= entity.config.max_token_count
        ):
            # Send and flush the buffer if appropriate
            entity.query_buffer.send_buffer()
        # Add binary data to list and update all counts
        entity.query_buffer.redis_token_count += len(entity.binary_entities)
        entity.query_buffer.buffer_size += added_size


################################################################################
# Bulk loader
################################################################################
# Command-line arguments
@click.command()
@click.argument("graph")
# Server connection settings
@click.option(
    "--server-url", "-u", default="redis://127.0.0.1:6379", help="Redis connection url"
)
@click.option("--nodes", "-n", multiple=True, help="Path to node csv file")
@click.option(
    "--nodes-with-label",
    "-N",
    nargs=2,
    multiple=True,
    help="Label string followed by path to node csv file",
)
@click.option("--relations", "-r", multiple=True, help="Path to relation csv file")
@click.option(
    "--relations-with-type",
    "-R",
    nargs=2,
    multiple=True,
    help="Relation type string followed by path to relation csv file",
)
@click.option(
    "--separator", "-o", default=",", help="Field token separator in csv file"
)
# Schema options
@click.option(
    "--enforce-schema",
    "-d",
    default=False,
    is_flag=True,
    help="Enforce the schema described in CSV header rows",
)
@click.option(
    "--id-type",
    "-j",
    default="STRING",
    help="The data type of unique node ID properties (either STRING or INTEGER)",
)
@click.option(
    "--skip-invalid-nodes",
    "-s",
    default=False,
    is_flag=True,
    help="ignore nodes that use previously defined IDs",
)
@click.option(
    "--skip-invalid-edges",
    "-e",
    default=False,
    is_flag=True,
    help="ignore invalid edges, print an error message and continue loading (True), or stop loading after an edge loading failure (False)",
)
@click.option(
    "--quote",
    "-q",
    default=0,
    help="the quoting format used in the CSV file. QUOTE_MINIMAL=0,QUOTE_ALL=1,QUOTE_NONNUMERIC=2,QUOTE_NONE=3",
)
@click.option(
    "--escapechar",
    "-x",
    default="\\",
    help='the escape char used for the CSV reader (default \\). Use "none" for None.',
)
# Buffer size restrictions
@click.option(
    "--max-token-count",
    "-c",
    default=1024,
    help="max number of processed CSVs to send per query (default 1024)",
)
@click.option(
    "--max-buffer-size",
    "-b",
    default=64,
    help="max buffer size in megabytes (default 64, max 1024)",
)
@click.option(
    "--max-token-size",
    "-t",
    default=64,
    help="max size of each token in megabytes (default 64, max 512)",
)
@click.option(
    "--index", "-i", multiple=True, help="Label:Propery on which to create an index"
)
@click.option(
    "--full-text-index",
    "-f",
    multiple=True,
    help="Label:Propery on which to create an full text search index",
)
@click.option(
    "--verbose",
    default=False,
    is_flag=True,
    help="Print extra information about the steps performed during loading",
)
def bulk_insert(
    graph,
    server_url,
    nodes,
    nodes_with_label,
    relations,
    relations_with_type,
    separator,
    enforce_schema,
    id_type,
    skip_invalid_nodes,
    skip_invalid_edges,
    escapechar,
    quote,
    max_token_count,
    max_buffer_size,
    max_token_size,
    index,
    full_text_index,
    verbose,
):
    if sys.version_info < (3, 10):
        raise RuntimeError("Python >= 3.10 is required for the falkordb bulk loader.")

    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        stream=sys.stdout,
        force=True,
    )

    # Allow operators to dump stack traces of all threads via `kill -SIGUSR1 <pid>`.
    register_stacktrace_dump_handler()

    if not (any(nodes) or any(nodes_with_label)):
        raise Exception("At least one node file must be specified.")

    start_time = timer()

    # If relations are being built, we must store unique node identifiers to later resolve endpoints.
    store_node_identifiers = any(relations) or any(relations_with_type)

    # Initialize configurations with command-line arguments
    config = Config(
        max_token_count,
        max_buffer_size,
        max_token_size,
        enforce_schema,
        id_type,
        skip_invalid_nodes,
        skip_invalid_edges,
        separator,
        int(quote),
        store_node_identifiers,
        escapechar,
    )

    logger.debug(f"Connecting to FalkorDB server at '{server_url}'...")

    client = FalkorDB.from_url(server_url)

    # Attempt to connect to the server
    try:
        client.connection.ping()
    except redis.exceptions.ConnectionError as e:
        logger.error("Could not connect to FalkorDB server.")
        raise e

    logger.debug("Connected to FalkorDB server.")

    # Attempt to verify that falkordb module is loaded
    try:
        module_list = [m["name"] for m in client.connection.module_list()]
        if "graph" not in module_list:
            logger.error("falkordb module not loaded on connected server.")
            sys.exit(1)
        logger.debug("FalkorDB module is loaded on the server.")
    except redis.exceptions.ResponseError:
        # Ignore check if the connected server does not support the "MODULE LIST" command
        logger.debug(
            "Server does not support 'MODULE LIST'; skipping FalkorDB module check."
        )

    # Verify that the graph name is not already used in the Redis database
    key_exists = client.connection.exists(graph)
    if key_exists:
        logger.error(
            f"Graph with name '{graph}', could not be created, as '{graph}' already exists."
        )
        sys.exit(1)

    logger.debug(f"Graph name '{graph}' is available.")

    query_buf = QueryBuffer(graph, client, config)

    # Read the header rows of each input CSV and save its schema.
    logger.debug("Parsing node CSV schemas...")
    labels = parse_schemas(Label, query_buf, nodes, nodes_with_label, config)
    logger.debug(f"Parsed {len(labels)} node CSV file(s).")
    logger.debug("Parsing relation CSV schemas...")
    reltypes = parse_schemas(
        RelationType, query_buf, relations, relations_with_type, config
    )
    logger.debug(f"Parsed {len(reltypes)} relation CSV file(s).")

    try:
        logger.debug("Processing nodes...")
        process_entities(labels)
        logger.debug("Processing relations...")
        process_entities(reltypes)
    except CSVError as e:
        sys.exit(str(e))

    # Send all remaining tokens to Redis
    logger.debug("Flushing remaining buffered data to FalkorDB...")
    query_buf.send_buffer()
    query_buf.wait_pool()

    end_time = timer()
    query_buf.report_completion(end_time - start_time)

    # Add in Graph Indices after graph creation
    graph = client.select_graph(graph)
    for i in index:
        l, p = i.split(":")
        logger.info(f"Creating Index on Label: {l}, Property: {p}")
        try:
            graph.create_node_range_index(l, p)
        except redis.exceptions.ResponseError as e:
            logger.error(f"Unable to create Index on Label: {l}, Property {p}")
            logger.error(e)

    # Add in Full Text Search Indices after graph creation
    for i in full_text_index:
        l, p = i.split(":")
        logger.info(f"Creating Full Text Search Index on Label: {l}, Property: {p}")
        try:
            graph.create_node_fulltext_index(l, p)
        except redis.exceptions.ResponseError as e:
            logger.error(
                f"Unable to create Full Text Search Index on Label: {l}, Property {p}"
            )
            logger.error(e)
        except Exception:
            logger.error(
                f"Unknown Error: Unable to create Full Text Search Index on Label: {l}, Property {p}"
            )


if __name__ == "__main__":
    bulk_insert()
