# QueryBuffer is the class that processes input CSVs and emits their binary formats to the Redis client.
class QueryBuffer(object):
    nodes = None
    top_node_id = 0

    def __init__(self, graphname, client, has_relations):
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

        # Create a node dictionary if we're building relations and as such require unique identifiers
        if has_relations:
            self.nodes = {}

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
