class Config:
    """Default values for command-line arguments"""
    max_token_count = 1024 * 1023
    max_buffer_size = 0
    max_token_size = 512 * 1000000
    enforce_schema = False
    skip_invalid_nodes = False
    skip_invalid_edges = False
    separator = ','
    quoting = 3
