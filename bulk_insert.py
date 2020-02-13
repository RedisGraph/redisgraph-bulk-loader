import csv
import os
import io
import sys
import math
import struct
import json
from timeit import default_timer as timer
import redis
import click

# Global variables
CONFIGS = None         # thresholds for batching Redis queries
NODE_DICT = {}         # global node dictionary
TOP_NODE_ID = 0        # next ID to assign to a node
QUERY_BUF = None       # Buffer for query being constructed
QUOTING = None

FIELD_TYPES = None

# Custom error class for invalid inputs
class CSVError(Exception):
    pass

# Official enum support varies widely between 2.7 and 3.x, so we'll use a custom class
class Type:
    NULL = 0
    BOOL = 1
    NUMERIC = 2
    STRING = 3

# User-configurable thresholds for when to send queries to Redis
class Configs(object):
    def __init__(self, max_token_count, max_buffer_size, max_token_size, skip_invalid_nodes, skip_invalid_edges):
        # Maximum number of tokens per query
        # 1024 * 1024 is the hard-coded Redis maximum. We'll set a slightly lower limit so
        # that we can safely ignore tokens that aren't binary strings
        # ("GRAPH.BULK", "BEGIN", graph name, counts)
        self.max_token_count = min(max_token_count, 1024 * 1023)
        # Maximum size in bytes per query
        self.max_buffer_size = max_buffer_size * 1000000
        # Maximum size in bytes per token
        # 512 megabytes is a hard-coded Redis maximum
        self.max_token_size = min(max_token_size * 1000000, 512 * 1000000)

        self.skip_invalid_nodes = skip_invalid_nodes
        self.skip_invalid_edges = skip_invalid_edges

# QueryBuffer is the class that processes input CSVs and emits their binary formats to the Redis client.
class QueryBuffer(object):
    def __init__(self, graphname, client):
        # Redis client and data for each query
        self.client = client

        # Sizes for buffer currently being constructed
        self.redis_token_count = 0
        self.buffer_size = 0

        # The first query should include a "BEGIN" token
        self.graphname = graphname
        self.initial_query = True

        self.node_count = 0
        self.relation_count = 0

        self.labels = [] # List containing all pending Label objects
        self.reltypes = [] # List containing all pending RelationType objects

        self.nodes_created = 0 # Total number of nodes created
        self.relations_created = 0 # Total number of relations created

    # Send all pending inserts to Redis
    def send_buffer(self):
        # Do nothing if we have no entities
        if self.node_count == 0 and self.relation_count == 0:
            return

        args = [self.node_count, self.relation_count, len(self.labels), len(self.reltypes)] + self.labels + self.reltypes
        # Prepend a "BEGIN" token if this is the first query
        if self.initial_query:
            args.insert(0, "BEGIN")
            self.initial_query = False

        result = self.client.execute_command("GRAPH.BULK", self.graphname, *args)
        stats = result.split(', '.encode())
        self.nodes_created += int(stats[0].split(' '.encode())[0])
        self.relations_created += int(stats[1].split(' '.encode())[0])

        self.clear_buffer()

    # Delete all entities that have been inserted
    def clear_buffer(self):
        self.redis_token_count = 0
        self.buffer_size = 0

        # All constructed entities have been inserted, so clear buffers
        self.node_count = 0
        self.relation_count = 0
        del self.labels[:]
        del self.reltypes[:]

    def report_completion(self, runtime):
        print("Construction of graph '%s' complete: %d nodes created, %d relations created in %f seconds"
              % (self.graphname, self.nodes_created, self.relations_created, runtime))

# Superclass for label and relation CSV files
class EntityFile(object):
    def __init__(self, filename, separator):
        # The label or relation type string is the basename of the file
        self.entity_str = os.path.splitext(os.path.basename(filename))[0]
        # Input file handling
        self.infile = io.open(filename, 'rt')
        # Initialize CSV reader that ignores leading whitespace in each field
        # and does not modify input quote characters
        self.reader = csv.reader(self.infile, delimiter=separator, skipinitialspace=True, quoting=QUOTING)

        self.prop_offset = 0 # Starting index of properties in row
        self.prop_count = 0 # Number of properties per entity

        self.packed_header = b''
        self.binary_entities = []
        self.binary_size = 0 # size of binary token
        self.count_entities() # number of entities/row in file.

    # Count number of rows in file.
    def count_entities(self):
        self.entities_count = 0
        self.entities_count = sum(1 for line in self.infile)
        # discard header row
        self.entities_count -= 1
        # seek back
        self.infile.seek(0)
        return self.entities_count

    # Simple input validations for each row of a CSV file
    def validate_row(self, expected_col_count, row):
        # Each row should have the same number of fields
        if len(row) != expected_col_count:
            raise CSVError("%s:%d Expected %d columns, encountered %d ('%s')"
                           % (self.infile.name, self.reader.line_num, expected_col_count, len(row), ','.join(row)))

    # If part of a CSV file was sent to Redis, delete the processed entities and update the binary size
    def reset_partial_binary(self):
        self.binary_entities = []
        self.binary_size = len(self.packed_header)

    # Convert property keys from a CSV file header into a binary string
    def pack_header(self, header):
        prop_count = len(header) - self.prop_offset
        # String format
        entity_bytes = self.entity_str.encode()
        fmt = "=%dsI" % (len(entity_bytes) + 1) # Unaligned native, entity name, count of properties
        args = [entity_bytes, prop_count]
        for p in header[self.prop_offset:]:
            prop = p.encode()
            fmt += "%ds" % (len(prop) + 1) # encode string with a null terminator
            args.append(prop)
        return struct.pack(fmt, *args)

    # Convert a list of properties into a binary string
    def pack_props(self, line):
        props = []
        for num, field in enumerate(line[self.prop_offset:]):
            field_type_idx = self.prop_offset+num
            try:
                FIELD_TYPES[self.entity_str][field_type_idx]
            except:
                props.append(prop_to_binary(field, None))
            else:
                props.append(prop_to_binary(field, FIELD_TYPES[self.entity_str][field_type_idx]))
        return b''.join(p for p in props)

    def to_binary(self):
        return self.packed_header + b''.join(self.binary_entities)

# Handler class for processing label csv files.
class Label(EntityFile):
    def __init__(self, infile, separator):
        super(Label, self).__init__(infile, separator)
        expected_col_count = self.process_header()
        self.process_entities(expected_col_count)
        self.infile.close()

    def process_header(self):
        # Header format:
        # node identifier (which may be a property key), then all other property keys
        header = next(self.reader)
        expected_col_count = len(header)
        # If identifier field begins with an underscore, don't add it as a property.
        if header[0][0] == '_':
            self.prop_offset = 1
        self.packed_header = self.pack_header(header)
        self.binary_size += len(self.packed_header)
        return expected_col_count

    def process_entities(self, expected_col_count):
        global NODE_DICT
        global TOP_NODE_ID
        global QUERY_BUF

        entities_created = 0
        with click.progressbar(self.reader, length=self.entities_count, label=self.entity_str) as reader:
            for row in reader:
                self.validate_row(expected_col_count, row)
                # Add identifier->ID pair to dictionary if we are building relations
                if NODE_DICT is not None:
                    if row[0] in NODE_DICT:
                        sys.stderr.write("Node identifier '%s' was used multiple times - second occurrence at %s:%d\n"
                                         % (row[0], self.infile.name, self.reader.line_num))
                        if CONFIGS.skip_invalid_nodes is False:
                            exit(1)
                    NODE_DICT[row[0]] = TOP_NODE_ID
                    TOP_NODE_ID += 1
                row_binary = self.pack_props(row)
                row_binary_len = len(row_binary)
                # If the addition of this entity will make the binary token grow too large,
                # send the buffer now.
                if self.binary_size + row_binary_len > CONFIGS.max_token_size:
                    QUERY_BUF.labels.append(self.to_binary())
                    QUERY_BUF.send_buffer()
                    self.reset_partial_binary()
                    # Push the label onto the query buffer again, as there are more entities to process.
                    QUERY_BUF.labels.append(self.to_binary())

                QUERY_BUF.node_count += 1
                entities_created += 1
                self.binary_size += row_binary_len
                self.binary_entities.append(row_binary)
            QUERY_BUF.labels.append(self.to_binary())
        print("%d nodes created with label '%s'" % (entities_created, self.entity_str))

# Handler class for processing relation csv files.
class RelationType(EntityFile):
    def __init__(self, infile, separator):
        super(RelationType, self).__init__(infile, separator)
        expected_col_count = self.process_header()
        self.process_entities(expected_col_count)
        self.infile.close()

    def process_header(self):
        # Header format:
        # source identifier, dest identifier, properties[0..n]
        header = next(self.reader)
        # Assume rectangular CSVs
        expected_col_count = len(header)
        self.prop_count = expected_col_count - 2
        if self.prop_count < 0:
            raise CSVError("Relation file '%s' should have at least 2 elements in header line."
                           % (self.infile.name))

        self.prop_offset = 2
        self.packed_header = self.pack_header(header) # skip src and dest identifiers
        self.binary_size += len(self.packed_header)
        return expected_col_count

    def process_entities(self, expected_col_count):
        entities_created = 0
        with click.progressbar(self.reader, length=self.entities_count, label=self.entity_str) as reader:
            for row in reader:
                self.validate_row(expected_col_count, row)
                try:
                    src = NODE_DICT[row[0]]
                    dest = NODE_DICT[row[1]]
                except KeyError as e:
                    print("Relationship specified a non-existent identifier. src: %s; dest: %s" % (row[0], row[1]))
                    if CONFIGS.skip_invalid_edges is False:
                        raise e
                    continue
                fmt = "=QQ" # 8-byte unsigned ints for src and dest
                row_binary = struct.pack(fmt, src, dest) + self.pack_props(row)
                row_binary_len = len(row_binary)
                # If the addition of this entity will make the binary token grow too large,
                # send the buffer now.
                if self.binary_size + row_binary_len > CONFIGS.max_token_size:
                    QUERY_BUF.reltypes.append(self.to_binary())
                    QUERY_BUF.send_buffer()
                    self.reset_partial_binary()
                    # Push the reltype onto the query buffer again, as there are more entities to process.
                    QUERY_BUF.reltypes.append(self.to_binary())

                QUERY_BUF.relation_count += 1
                entities_created += 1
                self.binary_size += row_binary_len
                self.binary_entities.append(row_binary)
            QUERY_BUF.reltypes.append(self.to_binary())
        print("%d relations created for type '%s'" % (entities_created, self.entity_str))

# Convert a single CSV property field into a binary stream.
# Supported property types are string, numeric, boolean, and NULL.
# type is either Type.NUMERIC, Type.BOOL or Type.STRING, and explicitly sets the value to this type if possible
def prop_to_binary(prop_val, type):
    # All format strings start with an unsigned char to represent our Type enum
    format_str = "=B"
    if prop_val is None:
        # An empty field indicates a NULL property
        return struct.pack(format_str, Type.NULL)

    # If field can be cast to a float, allow it
    if type is None or type == Type.NUMERIC:
        try:
            numeric_prop = float(prop_val)
            if not math.isnan(numeric_prop) and not math.isinf(numeric_prop): # Don't accept non-finite values.
                return struct.pack(format_str + "d", Type.NUMERIC, numeric_prop)
        except:
            pass

    if type is None or type == Type.BOOL:
        # If field is 'false' or 'true', it is a boolean
        if prop_val.lower() == 'false':
            return struct.pack(format_str + '?', Type.BOOL, False)
        elif prop_val.lower() == 'true':
            return struct.pack(format_str + '?', Type.BOOL, True)

    if type is None or type == Type.STRING:
        # If we've reached this point, the property is a string
        encoded_str = str.encode(prop_val) # struct.pack requires bytes objects as arguments
        # Encoding len+1 adds a null terminator to the string
        format_str += "%ds" % (len(encoded_str) + 1)
        return struct.pack(format_str, Type.STRING, encoded_str)

    ## if it hasn't returned by this point, it is trying to set it to a type that it can't adopt
    raise Exception("unable to parse [" + prop_val + "] with type ["+repr(type)+"]")

# For each node input file, validate contents and convert to binary format.
# If any buffer limits have been reached, flush all enqueued inserts to Redis.
def process_entity_csvs(cls, csvs, separator):
    global QUERY_BUF
    for in_csv in csvs:
        # Build entity descriptor from input CSV
        entity = cls(in_csv, separator)
        added_size = entity.binary_size
        # Check to see if the addition of this data will exceed the buffer's capacity
        if (QUERY_BUF.buffer_size + added_size >= CONFIGS.max_buffer_size
                or QUERY_BUF.redis_token_count + len(entity.binary_entities) >= CONFIGS.max_token_count):
            # Send and flush the buffer if appropriate
            QUERY_BUF.send_buffer()
        # Add binary data to list and update all counts
        QUERY_BUF.redis_token_count += len(entity.binary_entities)
        QUERY_BUF.buffer_size += added_size

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


def bulk_insert(graph, host, port, password, nodes, relations, separator, max_token_count, max_buffer_size, max_token_size, quote, field_types, skip_invalid_nodes, skip_invalid_edges):
    global CONFIGS
    global NODE_DICT
    global TOP_NODE_ID
    global QUERY_BUF
    global QUOTING
    global FIELD_TYPES

    if sys.version_info[0] < 3:
        raise Exception("Python 3 is required for the RedisGraph bulk loader.")

    if field_types is not None:
        try:
            FIELD_TYPES = json.loads(field_types)
        except:
            raise Exception("Problem parsing field-types. Use the format {<label>:[<col1 type>, <col2 type> ...]} where type can be 0(null),1(bool),2(numeric),3(string) ")

    QUOTING = int(quote)

    TOP_NODE_ID = 0 # reset global ID variable (in case we are calling bulk_insert from unit tests)
    CONFIGS = Configs(max_token_count, max_buffer_size, max_token_size, skip_invalid_nodes, skip_invalid_edges)

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
            exit(1)
    except redis.exceptions.ResponseError:
        # Ignore check if the connected server does not support the "MODULE LIST" command
        pass

    # Verify that the graph name is not already used in the Redis database
    key_exists = client.execute_command("EXISTS", graph)
    if key_exists:
        print("Graph with name '%s', could not be created, as Redis key '%s' already exists." % (graph, graph))
        exit(1)

    QUERY_BUF = QueryBuffer(graph, client)

    # Create a node dictionary if we're building relations and as such require unique identifiers
    if relations:
        NODE_DICT = {}
    else:
        NODE_DICT = None

    process_entity_csvs(Label, nodes, separator)

    if relations:
        process_entity_csvs(RelationType, relations, separator)

    # Send all remaining tokens to Redis
    QUERY_BUF.send_buffer()

    end_time = timer()
    QUERY_BUF.report_completion(end_time - start_time)

if __name__ == '__main__':
    bulk_insert()
