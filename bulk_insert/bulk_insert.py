import sys
import json
from timeit import default_timer as timer
import redis
import click
from configs import Configs
from query_buffer import QueryBuffer
from label import Label
from relation_type import RelationType
import module_vars


# For each node input file, validate contents and convert to binary format.
# If any buffer limits have been reached, flush all enqueued inserts to Redis.
def process_entity_csvs(cls, csvs, separator):
    for in_csv in csvs:
        # Build entity descriptor from input CSV
        entity = cls(in_csv, separator)
        added_size = entity.binary_size
        # Check to see if the addition of this data will exceed the buffer's capacity
        if (module_vars.QUERY_BUF.buffer_size + added_size >= module_vars.CONFIGS.max_buffer_size
                or module_vars.QUERY_BUF.redis_token_count + len(entity.binary_entities) >= module_vars.CONFIGS.max_token_count):
            # Send and flush the buffer if appropriate
            module_vars.QUERY_BUF.send_buffer()
        # Add binary data to list and update all counts
        module_vars.QUERY_BUF.redis_token_count += len(entity.binary_entities)
        module_vars.QUERY_BUF.buffer_size += added_size

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
@click.option('--field-types', '-f', default=None, help='json to set explicit types for each field, format {<label>:[<col1 type>, <col2 type> ...]} where type can be 0(null),1(bool),2(numeric),3(string)')
@click.option('--skip-invalid-nodes', '-s', default=False, is_flag=True, help='ignore nodes that use previously defined IDs')
@click.option('--skip-invalid-edges', '-e', default=False, is_flag=True, help='ignore invalid edges, print an error message and continue loading (True), or stop loading after an edge loading failure (False)')
@click.option('--enforce-schema', '-S', default=False, is_flag=True, help='header line introduces property schema')
def bulk_insert(graph, host, port, password, nodes, relations, separator, max_token_count, max_buffer_size, max_token_size, quote, field_types, skip_invalid_nodes, skip_invalid_edges, enforce_schema):
    if sys.version_info[0] < 3:
        raise Exception("Python 3 is required for the RedisGraph bulk loader.")

    if field_types is not None:
        try:
            module_vars.FIELD_TYPES = json.loads(field_types)
        except:
            raise Exception("Problem parsing field-types. Use the format {<label>:[<col1 type>, <col2 type> ...]} where type can be 0(null),1(bool),2(numeric),3(string) ")

    module_vars.QUOTING = int(quote)

    module_vars.TOP_NODE_ID = 0 # reset global ID variable (in case we are calling bulk_insert from unit tests)
    module_vars.CONFIGS = Configs(max_token_count, max_buffer_size, max_token_size, skip_invalid_nodes, skip_invalid_edges, enforce_schema)

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

    # If we're enforcing a schema, validate the headers in each file?
    if enforce_schema:
        pass

    module_vars.QUERY_BUF = QueryBuffer(graph, client)

    # Create a node dictionary if we're building relations and as such require unique identifiers
    if relations:
        module_vars.NODE_DICT = {}
    else:
        module_vars.NODE_DICT = None

    process_entity_csvs(Label, nodes, separator)

    if relations:
        process_entity_csvs(RelationType, relations, separator)

    # Send all remaining tokens to Redis
    module_vars.QUERY_BUF.send_buffer()

    end_time = timer()
    module_vars.QUERY_BUF.report_completion(end_time - start_time)


if __name__ == '__main__':
    bulk_insert()
