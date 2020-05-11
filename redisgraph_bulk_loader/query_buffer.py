# QueryBuffer is the singleton module that processes input CSVs and emits their binary formats to the Redis client.

nodes = None
top_node_id = 0

# Redis client and data for each query
client = None

# Sizes for buffer currently being constructed
redis_token_count = 0
buffer_size = 0

# The first query should include a "BEGIN" token
graphname = ""
initial_query = True

node_count = 0
relation_count = 0

labels = [] # List containing all pending Label objects
reltypes = [] # List containing all pending RelationType objects

nodes_created = 0 # Total number of nodes created
relations_created = 0 # Total number of relations created


# Send all pending inserts to Redis
def send_buffer():
    global initial_query
    global nodes_created
    global relations_created

    # Do nothing if we have no entities
    if node_count == 0 and relation_count == 0:
        return

    args = [node_count, relation_count, len(labels), len(reltypes)] + labels + reltypes
    # Prepend a "BEGIN" token if this is the first query
    if initial_query:
        args.insert(0, "BEGIN")
        initial_query = False

    result = client.execute_command("GRAPH.BULK", graphname, *args)
    stats = result.split(', '.encode())
    nodes_created += int(stats[0].split(' '.encode())[0])
    relations_created += int(stats[1].split(' '.encode())[0])

    clear_buffer()


# Delete all entities that have been inserted
def clear_buffer():
    global redis_token_count
    global buffer_size
    global node_count
    global relation_count
    global labels
    global reltypes

    redis_token_count = 0
    buffer_size = 0

    # All constructed entities have been inserted, so clear buffers
    node_count = 0
    relation_count = 0
    del labels[:]
    del reltypes[:]


def report_completion(runtime):
    print("Construction of graph '%s' complete: %d nodes created, %d relations created in %f seconds"
          % (graphname, nodes_created, relations_created, runtime))
