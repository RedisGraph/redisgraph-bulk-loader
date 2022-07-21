import sys
from timeit import default_timer as timer

import click
import redis

try:
    from .config import Config
    from .label import Label
    from .query_buffer import QueryBuffer
    from .relation_type import RelationType
except:
    from config import Config
    from label import Label
    from query_buffer import QueryBuffer
    from relation_type import RelationType

def parse_schemas(cls, query_buf, path_to_csv, csv_tuples, config, label_column):
    schemas = [None] * (len(path_to_csv) + len(csv_tuples))
    for idx, in_csv in enumerate(path_to_csv):
        # Build entity descriptor from input CSV
        schemas[idx] = cls(query_buf, in_csv, None, config, label_column)

    offset = len(path_to_csv)
    for idx, csv_tuple in enumerate(csv_tuples):
        # Build entity descriptor from input CSV
        schemas[idx + offset] = cls(query_buf, csv_tuple[1], csv_tuple[0], config, label_column)
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
# Redis server connection settings
@click.option(
    "--redis-url", "-u", default="redis://127.0.0.1:6379", help="Redis connection url"
)
@click.option("--nodes", "-n", multiple=True, help="Path to node csv file")
@click.option("--node-label-column", "-L", default=None, nargs=2, help="Import based on <column> having <value>")
@click.option(
    "--nodes-with-label",
    "-N",
    nargs=2,
    multiple=True,
    help="Label string followed by path to node csv file",
)
@click.option("--relations", "-r", multiple=True, help="Path to relation csv file")
@click.option("--relation-type-column", "-T", default=None, nargs=2, help="Import based on <column> having <value>")
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
def bulk_insert(
    graph,
    redis_url,
    nodes,
    node_label_column,
    nodes_with_label,
    relations,
    relation_type_column,
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
):
    
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

    client = redis.from_url(redis_url)

    # Attempt to connect to Redis server
    try:
        client.ping()
    except redis.exceptions.ConnectionError as e:
        print("Could not connect to Redis server.")
        raise e

    # Attempt to verify that RedisGraph module is loaded
    try:
        module_list = [m[b"name"] for m in client.module_list()]
        if b"graph" not in module_list:
            print("RedisGraph module not loaded on connected server.")
            sys.exit(1)
    except redis.exceptions.ResponseError:
        # Ignore check if the connected server does not support the "MODULE LIST" command
        pass

    # Verify that the graph name is not already used in the Redis database
    key_exists = client.execute_command("EXISTS", graph)
    if key_exists:
        print(
            f"Graph with name '{graph}', could not be created, as Redis key '{graph}' already exists."
        )
        sys.exit(1)

    query_buf = QueryBuffer(graph, client, config)

    # Read the header rows of each input CSV and save its schema.
    labels = parse_schemas(Label, query_buf, nodes, nodes_with_label, config, node_label_column)
    reltypes = parse_schemas(
        RelationType, query_buf, relations, relations_with_type, config, relation_type_column,
    )

    process_entities(labels)
    process_entities(reltypes)

    # Send all remaining tokens to Redis
    query_buf.send_buffer()
    query_buf.wait_pool()

    end_time = timer()
    query_buf.report_completion(end_time - start_time)

    # Add in Graph Indices after graph creation
    for i in index:
        l, p = i.split(":")
        print(f"Creating Index on Label: {l}, Property: {p}")
        try:
            index_create = client.execute_command(
                "GRAPH.QUERY", graph, f"CREATE INDEX ON :{l}({p})"
            )
            for z in index_create:
                print(z[0].decode("utf-8"))
        except redis.exceptions.ResponseError as e:
            print(f"Unable to create Index on Label: {l}, Property {p}")
            print(e)

    # Add in Full Text Search Indices after graph creation
    for i in full_text_index:
        l, p = i.split(":")
        print(f"Creating Full Text Search Index on Label: {l}, Property: {p}")
        try:
            index_create = client.execute_command(
                "GRAPH.QUERY",
                graph,
                f"CALL db.idx.fulltext.createNodeIndex('{l}', '{p}')",
            )
            print(index_create[-1][0].decode("utf-8"))
        except redis.exceptions.ResponseError as e:
            print(
                f"Unable to create Full Text Search Index on Label: {l}, Property {p}"
            )
            print(e)
        except Exception:
            print(
                f"Unknown Error: Unable to create Full Text Search Index on Label: {l}, Property {p}"
            )


if __name__ == "__main__":
    bulk_insert()
