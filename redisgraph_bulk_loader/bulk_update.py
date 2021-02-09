import io
import sys
import redis
import click
from timeit import default_timer as timer


# Count number of rows in file.
def count_entities(infile):
    entities_count = 0
    entities_count = sum(1 for line in infile)
    # seek back
    infile.seek(0)
    return entities_count


def process_update_csv(config, filename):
    # Input file handling
    infile = io.open(filename, 'rt')

    #  entity_count = count_entities(infile)

    # Initialize CSV reader that ignores leading whitespace in each field
    # and does not modify input quote characters
    #  reader = csv.reader(infile, delimiter=config.separator, skipinitialspace=True, quoting=config.quoting, escapechar='\\')

    next(infile) # skip header
    rows_str = "CYPHER rows=["
    rows_str += ",".join("[" + line.strip() + "]" for line in infile)
    #  with click.progressbar(infile, length=entity_count, label=filename) as reader:
        #  for row in reader:
            #  self.validate_row(row)
            #  rows_str += "[" + row + "]"
    rows_str += "]"

    return rows_str


################################################################################
# Bulk updater
################################################################################
# Command-line arguments
@click.command()
@click.argument('graph')
@click.option('--query', '-e', help='Query to run on server')
# Redis server connection settings
@click.option('--host', '-h', default='127.0.0.1', help='Redis server host')
@click.option('--port', '-p', default=6379, help='Redis server port')
@click.option('--password', '-a', default=None, help='Redis server password')
@click.option('--unix-socket-path', '-u', default=None, help='Redis server unix socket path')
# CSV file options
@click.option('--csv', '-c', help='Path to CSV file')
@click.option('--separator', '-o', default=',', help='Field token separator in csv file')
# Schema options
# Buffer size restrictions
@click.option('--max-token-size', '-t', default=500, help='max size of each token in megabytes (default 500, max 512)')
def bulk_update(graph, query, host, port, password, unix_socket_path, csv, separator, max_token_size):
    if sys.version_info[0] < 3:
        raise Exception("Python 3 is required for the RedisGraph bulk loader.")

    start_time = timer()

    # Attempt to connect to Redis server
    try:
        if unix_socket_path is not None:
            client = redis.StrictRedis(unix_socket_path=unix_socket_path, password=password)
        else:
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

    config = False
    rows = process_update_csv(config, csv)
    command = rows + " UNWIND $rows AS row " + query

    result = client.execute_command("GRAPH.QUERY", graph, command)

    end_time = timer()

    print("Update of graph '%s' complete in %f seconds" % (graph, end_time - start_time))
    print(result)


if __name__ == '__main__':
    bulk_update()
