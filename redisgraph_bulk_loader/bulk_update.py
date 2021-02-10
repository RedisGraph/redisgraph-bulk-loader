import io
import sys
import csv
import redis
import click
from redis import ResponseError
from timeit import default_timer as timer


def utf8len(s):
    return len(s.encode('utf-8'))


class BulkUpdate:
    """Handler class for emitting bulk update commands"""
    def __init__(self, graph, max_token_size, separator, no_header, filename, query, variable_name, client):
        self.separator = separator
        self.no_header = no_header
        self.query = " UNWIND $rows AS " + variable_name + " " + query
        self.max_token_size = max_token_size * 1024 * 1024 - utf8len(self.query)
        self.infile = io.open(filename, 'rt')
        self.graph = graph
        self.client = client
        self.statistics = {}

    # Count number of rows in file.
    def count_entities(self):
        entities_count = 0
        entities_count = sum(1 for line in self.infile)
        # seek back
        self.infile.seek(0)
        return entities_count

    def update_statistics(self, result):
        for raw_stat in result[0]:
            stat = raw_stat.split(": ")
            key = stat[0]
            try:
                val = self.statistics[key]
            except KeyError:
                val = 0
            val += float(stat[1].split(" ")[0])
            self.statistics[key] = val

    def emit_buffer(self, rows):
        command = rows + self.query
        try:
            result = self.client.execute_command("GRAPH.QUERY", self.graph, command)
        except ResponseError as e:
            raise e
        # If we encountered a run-time error, the last response element will be an exception.
        if isinstance(result[-1], ResponseError):
            raise result[-1]
        self.update_statistics(result)

    def quote_string(self, cell):
        cell = cell.strip()
        # Quote-interpolate cell if it is an unquoted string.
        try:
            float(cell) # Check for numeric
        except ValueError:
            if ((cell.lower() != 'false' and cell.lower() != 'true') and # Check for boolean
                    (cell[0] != '[' and cell.lower != ']') and # Check for array
                    (cell[0] != "\"" and cell[-1] != "\"") and # Check for double-quoted string
                    (cell[0] != "\'" and cell[-1] != "\'")): # Check for single-quoted string
                cell = "\"" + cell + "\""
        return cell

    def process_update_csv(self):
        entity_count = self.count_entities()

        if self.no_header is False:
            next(self.infile) # skip header

        reader = csv.reader(self.infile, delimiter=self.separator, skipinitialspace=True, quoting=csv.QUOTE_NONE, escapechar='\\')

        rows_str = "CYPHER rows=["
        first = True
        with click.progressbar(reader, length=entity_count, label=self.graph) as reader:
            for row in reader:
                # Prepare the string representation of the current row.
                row = ",".join([self.quote_string(cell) for cell in row])
                next_line = "[" + row.strip() + "]"

                # Emit buffer now if the max token size would be exceeded by this addition.
                if utf8len(rows_str) + utf8len(next_line) > self.max_token_size:
                    # Add a closing bracket
                    rows_str += "]"
                    self.emit_buffer(rows_str)
                    rows_str = "CYPHER rows=["
                    first = True

                # Add a comma separator if this is not the first row in the query.
                if not first:
                    rows_str += ","
                first = False

                # Concatenate the string into the rows string representation.
                rows_str += next_line
        # Add a closing bracket
        rows_str += "]"
        self.emit_buffer(rows_str)
        self.infile.close()


################################################################################
# Bulk updater
################################################################################
# Command-line arguments
@click.command()
@click.argument('graph')
# Redis server connection settings
@click.option('--host', '-h', default='127.0.0.1', help='Redis server host')
@click.option('--port', '-p', default=6379, help='Redis server port')
@click.option('--password', '-a', default=None, help='Redis server password')
@click.option('--unix-socket-path', '-u', default=None, help='Redis server unix socket path')
# Cypher query options
@click.option('--query', '-e', help='Query to run on server')
@click.option('--variable-name', '-v', default='row', help='Variable name for row array in queries (default: row)')
# CSV file options
@click.option('--csv', '-c', help='Path to CSV input file')
@click.option('--separator', '-o', default=',', help='Field token separator in CSV file')
@click.option('--no-header', '-n', default=False, is_flag=True, help='If set, the CSV file has no header')
# Buffer size restrictions
@click.option('--max-token-size', '-t', default=500, help='Max size of each token in megabytes (default 500, max 512)')
def bulk_update(graph, host, port, password, unix_socket_path, query, variable_name, csv, separator, no_header, max_token_size):
    if sys.version_info[0] < 3:
        raise Exception("Python 3 is required for the RedisGraph bulk updater.")

    start_time = timer()

    # Attempt to connect to Redis server
    try:
        if unix_socket_path is not None:
            client = redis.StrictRedis(unix_socket_path=unix_socket_path, password=password, decode_responses=True)
        else:
            client = redis.StrictRedis(host=host, port=port, password=password, decode_responses=True)
    except redis.exceptions.ConnectionError as e:
        print("Could not connect to Redis server.")
        raise e

    # Attempt to verify that RedisGraph module is loaded
    try:
        module_list = client.execute_command("MODULE LIST")
        if not any('graph' in module_description for module_description in module_list):
            print("RedisGraph module not loaded on connected server.")
            sys.exit(1)
    except redis.exceptions.ResponseError:
        # Ignore check if the connected server does not support the "MODULE LIST" command
        pass

    updater = BulkUpdate(graph, max_token_size, separator, no_header, csv, query, variable_name, client)
    updater.process_update_csv()

    end_time = timer()

    for key, value in updater.statistics.items():
        print(key + ": " + repr(value))
    print("Update of graph '%s' complete in %f seconds" % (graph, end_time - start_time))


if __name__ == '__main__':
    bulk_update()
