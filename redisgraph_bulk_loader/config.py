from .exceptions import SchemaError


class Config:
    def __init__(
        self,
        max_token_count=1024 * 1023,
        max_buffer_size=64,
        max_token_size=64,
        enforce_schema=False,
        id_type="STRING",
        skip_invalid_nodes=False,
        skip_invalid_edges=False,
        separator=",",
        quoting=3,
        store_node_identifiers=False,
        escapechar="\\",
    ):
        """Settings for this run of the bulk loader"""
        # Maximum number of tokens per query
        # 1024 * 1024 is the hard-coded Redis maximum. We'll set a slightly lower limit so
        # that we can safely ignore tokens that aren't binary strings
        # ("GRAPH.BULK", "BEGIN", graph name, counts)
        self.max_token_count = min(max_token_count, 1024 * 1023)
        # Maximum size in bytes per query
        self.max_buffer_size = min(max_buffer_size * 1_000_000, 1024 * 1_000_000)
        # Maximum size in bytes per token
        # 512 megabytes is a hard-coded Redis maximum
        self.max_token_size = min(
            max_token_size * 1_000_000, 512 * 1_000_000, self.max_buffer_size
        )

        self.enforce_schema = enforce_schema
        id_type = str.upper(id_type)
        if id_type != "STRING" and id_type != "INTEGER":
            raise SchemaError(
                "Specified invalid argument for --id-type, expected STRING or INTEGER"
            )
        self.id_type = id_type
        self.skip_invalid_nodes = skip_invalid_nodes
        self.skip_invalid_edges = skip_invalid_edges
        self.separator = separator
        self.quoting = quoting
        self.escapechar = None if escapechar.lower() == "none" else escapechar

        # True if we are building relations as well as nodes
        self.store_node_identifiers = store_node_identifiers
