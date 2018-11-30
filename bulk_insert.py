import redis
import csv
import os
import click
import errno
import ipdb # debug

working_dir = "working"
node_count = 0
relation_count = 0
nodefiles = []
relfiles = []

def process_node_csvs(csvs):
    global node_count
    global nodefiles
    node_dict = {}
    # A Label or Relationship name is set by the CSV file name
    # TODO validate name string
    for in_csv in csvs:
        filename = os.path.basename(in_csv)
        label = os.path.splitext(filename)[0]


        with open(os.path.join(working_dir, filename), 'w') as outfile, open(in_csv, 'rt') as infile:
            nodefiles.append(os.path.join(os.getcwd(), outfile.name))
            reader = csv.reader(infile)
            writer = csv.writer(outfile, quoting=csv.QUOTE_ALL)
            # Header format:
            # properties[0..n]
            header = next(reader)
            # Assume square CSVs
            expected_col_count = len(header) + 1 # prop_count + id field
            # Output format:
            # label name, properties[0..n]

            #  ipdb.set_trace()
            writer.writerow([label] + header)

            for row in reader:
                # Expect all entities to have the same property count
                if len(row) != expected_col_count:
                    raise CSVError ("%s:%d Expected %d columns, encountered %d ('%s')"
                            % (self.csvfile, self.reader.line_num, expected_col_count, len(row), ','.join(row)))
                # Check for dangling commma
                if (row[-1] == ''):
                    raise CSVError ("%s:%d Dangling comma in input. ('%s')"
                                    % (self.csvfile, self.reader.line_num, ','.join(row)))
                # Add identifier->ID pair to dictionary
                # TODO Check for duplications later
                node_dict[row[0]] = node_count
                node_count += 1
                # Add properties to CSV file
                writer.writerow(row[1:])

    return node_dict

def process_relation_csvs(csvs, node_dict):
    global relation_count
    global relfiles
    # A Label or Relationship name is set by the CSV file name
    # TODO validate name string
    for in_csv in csvs:
        filename = os.path.basename(in_csv)
        relation = os.path.splitext(filename)[0]

        with open(os.path.join(working_dir, filename), 'w') as outfile, open(in_csv, 'rt') as infile:
            relfiles.append(os.path.join(os.getcwd(), outfile.name))
            reader = csv.reader(infile)
            writer = csv.writer(outfile, quoting=csv.QUOTE_ALL)
            # Header format:
            # properties[0..n]
            #  header = next(reader)
            # Assume square CSVs
            #  expected_col_count = len(header) + 1 # prop_count + id field

            # Output format:
            # relation type name
            writer.writerow([relation])
            for row in reader:
                # TODO Support edge properties
                # Each row should have two columns (a source and dest ID)
                if len(row) != 2:
                   raise CSVError ("%s:%d Expected 2 columns, encountered %d ('%s')"
                                % (filename, self.reader.line_num, len(row), ','.join(row)))
                # Check for dangling commma
                if (row[-1] == ''):
                    raise CSVError ("%s:%d Dangling comma in input. ('%s')"
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
    global working_dir
    working_dir = "working_" + graph
    try:
        os.mkdir(working_dir)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise
    node_dict = process_node_csvs(nodes)

    if relations:
        process_relation_csvs(relations, node_dict)

    args = [graph, node_count, relation_count, "NODES"] + nodefiles
    if relation_count > 0:
        args += ["RELATIONS"] + relfiles


    redis_client = redis.StrictRedis(host=host, port=port, password=password)
    result = redis_client.execute_command("GRAPH.BULK", *args)
    print(result)

if __name__ == '__main__':
    bulk_insert()
