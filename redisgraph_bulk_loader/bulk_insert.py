import os
import sys
import ssl
import redis
import click
from timeit import default_timer as timer

sys.path.append(os.path.dirname(__file__))
from config import Config
from query_buffer import QueryBuffer
from label import Label
from relation_type import RelationType


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
        if (entity.query_buffer.buffer_size + added_size >= entity.config.max_buffer_size
                or entity.query_buffer.redis_token_count + len(entity.binary_entities) >= entity.config.max_token_count):
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
@click.argument('graph')
# Redis server connection settings
@click.option('--host', '-h', default='127.0.0.1', help='Redis server host')
@click.option('--port', '-p', default=6379, help='Redis server port')
@click.option('--password', '-a', default=None, help='Redis server password')
@click.option('--user', '-w', default=None, help='Username for Redis ACL')
@click.option('--unix-socket-path', '-u', default=None, help='Redis server unix socket path')
@click.option('--ssl-keyfile', '-k', default=None, help='SSL keyfile')
@click.option('--ssl-certfile', '-l', default=None, help='SSL certfile')
@click.option('--ssl-ca-certs', '-m', default=None, help='SSL CA certs')
# CSV file paths
@click.option('--nodes', '-n', multiple=True, help='Path to node csv file')
@click.option('--nodes-with-label', '-N', nargs=2, multiple=True, help='Label string followed by path to node csv file')
@click.option('--relations', '-r', multiple=True, help='Path to relation csv file')
@click.option('--relations-with-type', '-R', nargs=2, multiple=True, help='Relation type string followed by path to relation csv file')
@click.option('--separator', '-o', default=',', help='Field token separator in csv file')
# Schema options
@click.option('--enforce-schema', '-d', default=False, is_flag=True, help='Enforce the schema described in CSV header rows')
@click.option('--id-type', '-j', default='STRING', help='The data type of unique node ID properties (either STRING or INTEGER)')
@click.option('--skip-invalid-nodes', '-s', default=False, is_flag=True, help='ignore nodes that use previously defined IDs')
@click.option('--skip-invalid-edges', '-e', default=False, is_flag=True, help='ignore invalid edges, print an error message and continue loading (True), or stop loading after an edge loading failure (False)')
@click.option('--quote', '-q', default=0, help='the quoting format used in the CSV file. QUOTE_MINIMAL=0,QUOTE_ALL=1,QUOTE_NONNUMERIC=2,QUOTE_NONE=3')
@click.option('--escapechar', '-x', default='\\', help='the escape char used for the CSV reader (default \\). Use "none" for None.')
# Buffer size restrictions
@click.option('--max-token-count', '-c', default=1024, help='max number of processed CSVs to send per query (default 1024)')
@click.option('--max-buffer-size', '-b', default=64, help='max buffer size in megabytes (default 64, max 1024)')
@click.option('--max-token-size', '-t', default=64, help='max size of each token in megabytes (default 64, max 512)')
@click.option('--index', '-i', multiple=True, help='Label:Propery on which to create an index')
@click.option('--full-text-index', '-f', multiple=True, help='Label:Propery on which to create an full text search index')
def bulk_insert(graph, host, port, password, user, unix_socket_path, ssl_keyfile, ssl_certfile, ssl_ca_certs, nodes, nodes_with_label, relations, relations_with_type, separator, enforce_schema, id_type, skip_invalid_nodes, skip_invalid_edges, escapechar, quote, max_token_count, max_buffer_size, max_token_size, index, full_text_index):
    if sys.version_info.major < 3 or sys.version_info.minor < 6:
        raise Exception("Python >= 3.6 is required for the RedisGraph bulk loader.")

    if not (any(nodes) or any(nodes_with_label)):
        raise Exception("At least one node file must be specified.")

    start_time = timer()

    # If relations are being built, we must store unique node identifiers to later resolve endpoints.
    store_node_identifiers = any(relations) or any(relations_with_type)

    # Initialize configurations with command-line arguments
    config = Config(max_token_count, max_buffer_size, max_token_size, enforce_schema, id_type, skip_invalid_nodes, skip_invalid_edges, separator, int(quote), store_node_identifiers, escapechar)

    kwargs = {
        'host': host,
        'port': port,
        'username': user,
        'password': password
    }

    if unix_socket_path is not None:
        kwargs.update({
            'unix_socket_path': unix_socket_path
        })

    if ssl_keyfile or ssl_certfile or ssl_ca_certs:
        kwargs.update({
            'ssl': True,
            'ssl_keyfile': ssl_keyfile,
            'ssl_certfile': ssl_certfile,
            'ssl_cert_reqs': ssl.CERT_REQUIRED,
            'ssl_ca_certs': ssl_ca_certs
        })

    # Attempt to connect to Redis server
    try:
        client = redis.Redis(**kwargs)
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

    query_buf = QueryBuffer(graph, client, config)

    # Read the header rows of each input CSV and save its schema.
    labels = parse_schemas(Label, query_buf, nodes, nodes_with_label, config)
    reltypes = parse_schemas(RelationType, query_buf, relations, relations_with_type, config)

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
        print("Creating Index on Label: %s, Property: %s" % (l, p))
        try:
            index_create = client.execute_command("GRAPH.QUERY", graph, "CREATE INDEX ON :%s(%s)" % (l, p))
            for z in index_create:
                print(z[0].decode("utf-8"))
        except redis.exceptions.ResponseError as e:
            print("Unable to create Index on Label: %s, Property %s" % (l, p))
            print(e)

    # Add in Full Text Search Indices after graph creation
    for i in full_text_index:
        l, p = i.split(":")
        print("Creating Full Text Search Index on Label: %s, Property: %s" % (l, p))
        try:
            index_create = client.execute_command("GRAPH.QUERY", graph, "CALL db.idx.fulltext.createNodeIndex('%s', '%s')" % (l, p))
            print(index_create[-1][0].decode("utf-8"))
        except redis.exceptions.ResponseError as e:
            print("Unable to create Full Text Search Index on Label: %s, Property %s" % (l, p))
            print(e)
        except:
            print("Unknown Error: Unable to create Full Text Search Index on Label: %s, Property %s" % (l, p))


if __name__ == '__main__':
    bulk_insert()
