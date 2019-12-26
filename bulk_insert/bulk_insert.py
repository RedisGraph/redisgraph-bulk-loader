import sys
from timeit import default_timer as timer
import redis
import click
import configs
import query_buffer as QueryBuffer
from label import Label
from relation_type import RelationType


def parse_schemas(cls, csvs):
    schemas = [None] * len(csvs)
    for idx, in_csv in enumerate(csvs):
        # Build entity descriptor from input CSV
        schemas[idx] = cls(in_csv)
    return schemas


# For each input file, validate contents and convert to binary format.
# If any buffer limits have been reached, flush all enqueued inserts to Redis.
def process_entities(entities):
    for entity in entities:
        entity.process_entities()
        added_size = entity.binary_size
        # Check to see if the addition of this data will exceed the buffer's capacity
        if (QueryBuffer.buffer_size + added_size >= configs.max_buffer_size
                or QueryBuffer.redis_token_count + len(entity.binary_entities) >= configs.max_token_count):
            # Send and flush the buffer if appropriate
            QueryBuffer.send_buffer()
        # Add binary data to list and update all counts
        QueryBuffer.redis_token_count += len(entity.binary_entities)
        QueryBuffer.buffer_size += added_size


def Config_Set(max_token_count, max_buffer_size, max_token_size, skip_invalid_nodes, skip_invalid_edges, separator, quoting):
    # Maximum number of tokens per query
    # 1024 * 1024 is the hard-coded Redis maximum. We'll set a slightly lower limit so
    # that we can safely ignore tokens that aren't binary strings
    # ("GRAPH.BULK", "BEGIN", graph name, counts)
    configs.max_token_count = min(max_token_count, 1024 * 1023)
    # Maximum size in bytes per query
    configs.max_buffer_size = max_buffer_size * 1000000
    # Maximum size in bytes per token
    # 512 megabytes is a hard-coded Redis maximum
    configs.max_token_size = min(max_token_size * 1000000, 512 * 1000000)

    configs.skip_invalid_nodes = skip_invalid_nodes
    configs.skip_invalid_edges = skip_invalid_edges
    configs.separator = separator
    configs.quoting = quoting


def QueryBuf_Set(graphname, client, has_relations):
    # Redis client and data for each query
    QueryBuffer.client = client
    QueryBuffer.graphname = graphname

    # Create a node dictionary if we're building relations and as such require unique identifiers
    if has_relations:
        QueryBuffer.nodes = {}


# Command-line arguments
@click.command()
@click.argument('graph')
# Redis server connection settings
@click.option('--host', '-h', default='127.0.0.1', help='Redis server host')
@click.option('--port', '-p', default=6379, help='Redis server port')
@click.option('--password', '-a', default=None, help='Redis server password')
# CSV file paths
@click.option('--nodes', '-n', required=True, multiple=True, help='Path to node csv file')
@click.option('--relations', '-r', multiple=True, help='Path to relation csv file')
@click.option('--separator', '-o', default=',', help='Field token separator in csv file')
# Buffer size restrictions
@click.option('--max-token-count', '-c', default=1024, help='max number of processed CSVs to send per query (default 1024)')
@click.option('--max-buffer-size', '-b', default=2048, help='max buffer size in megabytes (default 2048)')
@click.option('--max-token-size', '-t', default=500, help='max size of each token in megabytes (default 500, max 512)')
@click.option('--quote', '-q', default=3, help='the quoting format used in the CSV file. QUOTE_MINIMAL=0,QUOTE_ALL=1,QUOTE_NONNUMERIC=2,QUOTE_NONE=3')
@click.option('--skip-invalid-nodes', '-s', default=False, is_flag=True, help='ignore nodes that use previously defined IDs')
@click.option('--skip-invalid-edges', '-e', default=False, is_flag=True, help='ignore invalid edges, print an error message and continue loading (True), or stop loading after an edge loading failure (False)')
def bulk_insert(graph, host, port, password, nodes, relations, separator, max_token_count, max_buffer_size, max_token_size, quote, skip_invalid_nodes, skip_invalid_edges):
    if sys.version_info[0] < 3:
        raise Exception("Python 3 is required for the RedisGraph bulk loader.")

    # Initialize configurations with command-line arguments
    Config_Set(max_token_count, max_buffer_size, max_token_size, skip_invalid_nodes, skip_invalid_edges, separator, int(quote))

    start_time = timer()
    # Attempt to connect to Redis server
    try:
        client = redis.StrictRedis(host=host, port=port, password=password)
    except redis.exceptions.ConnectionError as e:
        print("Could not connect to Redis server.")
        raise e

    # Attempt to verify that RedisGraph module is loaded
    try:
        module_list = client.execute_command("MODULE LIST")
        if not any(b'graph' in module_description for module_description in module_list):
            print("RedisGraph module not loaded on connected server.")
            sys.exit(1)
    except redis.exceptions.ResponseError:
        # Ignore check if the connected server does not support the "MODULE LIST" command
        pass

    # Verify that the graph name is not already used in the Redis database
    key_exists = client.execute_command("EXISTS", graph)
    if key_exists:
        print("Graph with name '%s', could not be created, as Redis key '%s' already exists." % (graph, graph))
        sys.exit(1)

    QueryBuf_Set(graph, client, relations is not None)

    # Read the header rows of each input CSV and save its schema.
    labels = parse_schemas(Label, nodes)
    reltypes = parse_schemas(RelationType, relations)

    process_entities(labels)

    if relations:
        process_entities(reltypes)

    # Send all remaining tokens to Redis
    QueryBuffer.send_buffer()

    end_time = timer()
    QueryBuffer.report_completion(end_time - start_time)


if __name__ == '__main__':
    bulk_insert()
