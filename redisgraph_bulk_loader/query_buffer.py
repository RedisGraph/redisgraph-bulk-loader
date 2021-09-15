from pathos.pools import ThreadPool as Pool

def run(client, graphname, args):
    result = client.execute_command("GRAPH.BULK", graphname, *args)
    stats = result.split(', '.encode())
    return stats

class QueryBuffer:
    def __init__(self, graphname, client, config):
        self.nodes = None
        self.top_node_id = 0

        # Redis client and data for each query
        self.client = client
        self.graphname = graphname

        # Create a node dictionary if we're building relations and as such require unique identifiers
        if config.store_node_identifiers:
            self.nodes = {}
        else:
            self.nodes = None

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

        self.pool = Pool(nodes=1)
        self.tasks = []

    def send_buffer(self):
        """Send all pending inserts to Redis"""
        # Do nothing if we have no entities
        if self.node_count == 0 and self.relation_count == 0:
            return

        args = [self.node_count, self.relation_count, len(self.labels), len(self.reltypes)] + self.labels + self.reltypes
        # Prepend a "BEGIN" token if this is the first query
        if self.initial_query:
            args.insert(0, "BEGIN")
            self.initial_query = False

        task = self.pool.apipe(run, self.client, self.graphname, args)
        self.add_task(task)

        self.clear_buffer()

    # Delete all entities that have been inserted
    def clear_buffer(self):
        del self.labels[:]
        del self.reltypes[:]

        self.redis_token_count = 0
        self.buffer_size = 0
        self.node_count = 0
        self.relation_count = 0

    def add_task(self, task):
        self.tasks.append(task)
        if len(self.tasks) == 5:
            task = self.tasks.pop(0)
            stats = task.get()
            self.update_stats(stats)

    def wait_pool(self):
        for task in self.tasks:
            stats = task.get()
            self.update_stats(stats)
        self.tasks.clear()

    def update_stats(self, stats):
        self.nodes_created += int(stats[0].split(' '.encode())[0])
        self.relations_created += int(stats[1].split(' '.encode())[0])

    def report_completion(self, runtime):
        print("Construction of graph '%s' complete: %d nodes created, %d relations created in %f seconds"
              % (self.graphname, self.nodes_created, self.relations_created, runtime))
