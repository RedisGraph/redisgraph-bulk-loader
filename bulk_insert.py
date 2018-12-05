import csv
import os
import errno
import struct
import redis
import click
import ipdb # debug

# Custom error class for invalid inputs
class CSVError(Exception):
    pass

# Official enum support varies widely between 2.7 and 3.x, so we'll use a custom class
class Type:
    NULL = 0
    BOOL = 1
    NUMERIC = 2
    STRING = 3

# String, numeric, boolean, or NULL
class Property:
    def __init__(self, prop_str):
        self.orig = prop_str
        # All format strings start with an unsigned char to represent our Type enum
        self.format_str = "=B"

        if not prop_str:
            # An empty field indicates a NULL property
            self.type = Type.NULL
            self.pack_args = []
            return

        # If field can be cast to a float, allow it
        try:
            self.pack_args = [float(prop_str)]
            self.type = Type.NUMERIC
            self.format_str += "d"
            return
        except: # TODO necessary?
            pass

        # If field is 'false' or 'true', it is a boolean
        if self.orig.lower() == 'false':
            self.type = Type.BOOL
            self.format_str += "?"
            self.pack_args = [False]
            return
        if self.orig.lower() == 'true':
            self.type = Type.BOOL
            self.format_str += "?"
            self.pack_args = [True]
            return

        # If we've reached this point, the property is a string
        self.type = Type.STRING
        self.format_str += "I%ds" % len(prop_str) # 4-byte int for string length, then string
        self.pack_args = [len(prop_str), prop_str]

    def to_binary(self):
        #  print "format: %s\tsize: %d" % (self.format_str, struct.calcsize(self.format_str))
        return struct.pack(self.format_str, *[self.type] + self.pack_args)

WORKING_DIR = "working_"
NODE_COUNT = 0
RELATION_COUNT = 0
NODEFILES = []
RELFILES = []

def pack_header(label, line):
    prop_count = len(line)
    # String format
    # Is == length, string
    fmt = "=I%dsI" % len(label) # Unaligned native, label length, label string, count of properties
    args = [len(label), label, prop_count]
    for prop in line:
        fmt += "I%ds" % len(prop)
        args += [len(prop), prop]
    return struct.pack(fmt, *args)

def pack_node(line):
    #  prop_count = len(line)
    props = []
    #  struct.pack()
    for field in line:
        props.append(Property(field))

    return props

def process_node_csvs(csvs):
    global NODE_COUNT
    global NODEFILES
    node_dict = {}
    # A Label or Relationship name is set by the CSV file name
    # TODO validate name string
    for in_csv in csvs:
        filename = os.path.basename(in_csv)
        label = os.path.splitext(filename)[0].encode("ascii")

        with open(os.path.join(WORKING_DIR, label + ".dat"), 'wb') as outfile, open(in_csv, 'rt') as infile:
            NODEFILES.append(os.path.join(os.getcwd(), outfile.name))
            # Initialize CSV reader that ignores leading whitespace in each field and does not modify input quote characters
            reader = csv.reader(infile, skipinitialspace=True, quoting=csv.QUOTE_NONE)
            # Header format:
            # identifier, properties[0..n]
            header = next(reader)
            # Assume rectangular CSVs
            expected_col_count = len(header) # id field + prop_count
            # TODO verify that header is not empty

            properties_start = 0
            # If identifier field begins with an underscore, don't add it as a property.
            if header[0][0] == '_':
                properties_start = 1

            out = pack_header(label, header[properties_start:])
            outfile.write(out)
            #  ipdb.set_trace()

            for row in reader:
                # Expect all entities to have the same property count
                if len(row) != expected_col_count:
                    raise CSVError("%s:%d Expected %d columns, encountered %d ('%s')"
                                   % (infile.name, reader.line_num, expected_col_count, len(row), ','.join(row)))
                # Check for dangling commma
                if row[-1] == ',':
                    raise CSVError("%s:%d Dangling comma in input. ('%s')"
                                   % (infile.name, reader.line_num, ','.join(row)))
                # Add identifier->ID pair to dictionary
                # TODO Check for duplications later
                node_dict[row[0]] = NODE_COUNT
                NODE_COUNT += 1
                out = pack_node(row[properties_start:])
                for prop in out:
                    outfile.write(prop.to_binary())

    return node_dict

def process_relation_csvs(csvs, node_dict):
    global RELATION_COUNT
    global RELFILES
    # A Label or Relationship name is set by the CSV file name
    # TODO validate name string
    for in_csv in csvs:
        filename = os.path.basename(in_csv)
        relation = os.path.splitext(filename)[0]

        with open(os.path.join(WORKING_DIR, filename), 'w') as outfile, open(in_csv, 'rt') as infile:
            RELFILES.append(os.path.join(os.getcwd(), outfile.name))
            reader = csv.reader(infile)
            writer = csv.writer(outfile, quoting=csv.QUOTE_ALL)
            # Header format:
            # properties[0..n]
            #  header = next(reader)
            # Assume rectangular CSVs
            #  expected_col_count = len(header) + 1 # prop_count + id field

            # Output format:
            # relation type name
            writer.writerow([relation])
            for row in reader:
                # TODO Support edge properties
                # Each row should have two columns (a source and dest ID)
                if len(row) != 2:
                    raise CSVError("%s:%d Expected 2 columns, encountered %d ('%s')"
                                   % (filename, self.reader.line_num, len(row), ','.join(row)))
                # Check for dangling commma
                if (row[-1] == ''):
                    raise CSVError("%s:%d Dangling comma in input. ('%s')"
                                    % (self.csvfile, self.reader.line_num, ','.join(row)))
                # Retrieve src and dest iDs from hash
                src = node_dict[row[0]]
                dest = node_dict[row[1]]
                relation_count += 1
                # Add properties to CSV file
                outrow = [src, dest]
                writer.writerow(outrow)


def help():
    pass

# Command-line arguments
@click.command()
@click.argument('graph')
# Redis server connection settings
@click.option('--host', '-h', default='127.0.0.1', help='Redis server host')
@click.option('--port', '-p', default=6379, help='Redis server port')
@click.option('--password', '-P', default=None, help='Redis server password')
# CSV file paths
@click.option('--nodes', '-n', required=True, multiple=True, help='path to node csv file')
@click.option('--relations', '-r', multiple=True, help='path to relation csv file')

def bulk_insert(graph, host, port, password, nodes, relations):
    global WORKING_DIR
    WORKING_DIR += graph
    try:
        os.mkdir(WORKING_DIR)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise
    node_dict = process_node_csvs(nodes)

    if relations:
        process_relation_csvs(relations, node_dict)

    args = [graph, NODE_COUNT, RELATION_COUNT, "NODES"] + NODEFILES
    if RELATION_COUNT > 0:
        args += ["RELATIONS"] + RELFILES

    redis_client = redis.StrictRedis(host=host, port=port, password=password)
    result = redis_client.execute_command("GRAPH.BULK", *args)
    print(result)
    # TODO Delete working dir

if __name__ == '__main__':
    bulk_insert()
