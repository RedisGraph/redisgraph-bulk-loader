# User-configurable thresholds for when to send queries to Redis
class Configs(object):
    def __init__(self, max_token_count, max_buffer_size, max_token_size, skip_invalid_nodes, skip_invalid_edges, enforce_schema):
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

        self.enforce_schema = enforce_schema
