import asyncio

class InternalBuffer:
    def __init__(self, graphname, client):
        self.client = client
        self.graphname = graphname

        # Sizes for buffer currently being constructed
        self.redis_token_count = 0
        self.buffer_size = 0

        self.node_count = 0
        self.relation_count = 0

        self.labels = [] # List containing all pending Label objects
        self.reltypes = [] # List containing all pending RelationType objects
    
    def send_buffer(self, initial_query):
        """Send all pending inserts to Redis"""
        # Do nothing if we have no entities
        if self.node_count == 0 and self.relation_count == 0:
            return None

        args = [self.node_count, self.relation_count, len(self.labels), len(self.reltypes)] + self.labels + self.reltypes
        # Prepend a "BEGIN" token if this is the first query
        if initial_query:
            args.insert(0, "BEGIN")

        return self.client.execute_command("GRAPH.BULK", self.graphname, *args)

class QueryBuffer:
    def __init__(self, graphname, client, config, async_requests):

        self.client = client
        self.graphname = graphname
        self.config = config
        self.async_requests = async_requests

        # A queue of internal buffers
        self.internal_buffers = list()
        for i in range(async_requests):
            self.internal_buffers.append(InternalBuffer(graphname, client))
        # Each buffer sent to RedisGraph returns awaitable
        self.awaitables = set()
        # Pop the first buffer
        self.current_buffer = self.internal_buffers.pop(0)

        self.initial_query = True
        self.nodes_created = 0 # Total number of nodes created
        self.relations_created = 0 # Total number of relations created

        self.nodes = None
        self.top_node_id = 0
        # Create a node dictionary if we're building relations and as such require unique identifiers
        if config.store_node_identifiers:
            self.nodes = {}
        else:
            self.nodes = None

    async def send_buffer(self, flush=False):
        # If flush is needed all of the awaitables need to be complete, otherwise at least one is needed.
        return_when_flag = asyncio.ALL_COMPLETED if flush is True else asyncio.FIRST_COMPLETED
        awaitable = self.current_buffer.send_buffer(self.initial_query)
        if awaitable is not None:
            self.awaitables.add(awaitable)
        # Requests are flushed and awaited when:
        # 1. Flush is needed.
        # 2. Initial query with BEGIN token, to avoid race condition on async RedisGraph servers.
        # 3. The amount of async requests has reached the limit.
        if(len(self.awaitables) == self.async_requests or self.initial_query is True or flush == True):
            done, pending = await asyncio.wait(self.awaitables, return_when = return_when_flag)
            for d in done:
                result = d.result()
                stats = result.split(', '.encode())
                self.nodes_created += int(stats[0].split(' '.encode())[0])
                self.relations_created += int(stats[1].split(' '.encode())[0])
                # Create a new buffer of each completed task.
                self.internal_buffers.append(InternalBuffer(self.graphname, self.client))
            # Store the pending tasks.
            self.awaitables = pending
            self.initial_query = False
        # Pop a new buffer.
        self.current_buffer = self.internal_buffers.pop(0)

    async def flush(self):
        await self.send_buffer(flush=True)

    def report_completion(self, runtime):
        print("Construction of graph '%s' complete: %d nodes created, %d relations created in %f seconds"
              % (self.graphname, self.nodes_created, self.relations_created, runtime))

    @property
    def node_count(self):
        return self.current_buffer.node_count

    @node_count.setter
    def node_count(self, value):
        self.current_buffer.node_count = value

    @property
    def buffer_size(self):
        return self.current_buffer.buffer_size
    
    @property
    def labels(self):
        return self.current_buffer.labels

    @property
    def reltypes(self):
        return self.current_buffer.reltypes

    @property
    def relation_count(self):
        return self.current_buffer.relation_count

    @relation_count.setter
    def relation_count(self, value):
        self.current_buffer.relation_count = value
    
    @property
    def redis_token_count(self):
        return self.current_buffer.redis_token_count

    @redis_token_count.setter
    def redis_token_count(self, value):
        self.current_buffer.redis_token_count = value

    @property
    def buffer_size(self):
        return self.current_buffer.buffer_size

    @buffer_size.setter
    def buffer_size(self, value):
        self.current_buffer.buffer_size = value
