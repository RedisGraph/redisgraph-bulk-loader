class Config:
    """Default values for command-line arguments"""
    max_token_count = 1024 * 1023
    max_buffer_size = 2_048_000_000
    max_token_size = 512_000_000
    enforce_schema = False
    skip_invalid_nodes = False
    skip_invalid_edges = False
    store_node_identifiers = False
    separator = ','
    quoting = 3


def Config_Set(max_token_count, max_buffer_size, max_token_size, enforce_schema, skip_invalid_nodes, skip_invalid_edges, separator, quoting):
    """Settings for this run of the bulk loader"""
    # Maximum number of tokens per query
    # 1024 * 1024 is the hard-coded Redis maximum. We'll set a slightly lower limit so
    # that we can safely ignore tokens that aren't binary strings
    # ("GRAPH.BULK", "BEGIN", graph name, counts)
    Config.max_token_count = min(max_token_count, 1024 * 1023)
    # Maximum size in bytes per query
    Config.max_buffer_size = max_buffer_size * 1_000_000
    # Maximum size in bytes per token
    # 512 megabytes is a hard-coded Redis maximum
    Config.max_token_size = min(max_token_size * 1_000_000, 512 * 1_000_000)

    Config.enforce_schema = enforce_schema
    Config.skip_invalid_nodes = skip_invalid_nodes
    Config.skip_invalid_edges = skip_invalid_edges
    Config.separator = separator
    Config.quoting = quoting
