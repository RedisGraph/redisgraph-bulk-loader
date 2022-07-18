import unittest

from redisgraph_bulk_loader.config import Config


class TestBulkLoader:
    def test_default_values(self):
        """Verify the default values in the Config class."""
        config = Config()
        assert config.max_token_count == 1024 * 1023
        assert config.max_buffer_size == 64_000_000
        assert config.max_token_size == 64_000_000
        assert config.enforce_schema == False
        assert config.id_type == "STRING"
        assert not config.skip_invalid_nodes
        assert not config.skip_invalid_edges
        assert not config.store_node_identifiers
        assert config.separator == ","
        assert config.quoting == 3

    def test_modified_values(self):
        """Verify that Config_set updates Config class values accordingly."""
        config = Config(
            max_token_count=10,
            max_buffer_size=500,
            max_token_size=200,
            enforce_schema=True,
            id_type="INTEGER",
            skip_invalid_nodes=True,
            skip_invalid_edges=True,
            separator="|",
            quoting=0,
        )
        assert config.max_token_count == 10
        assert config.max_token_size == 200_000_000
        # Max token size argument is converted to megabytes
        assert config.max_buffer_size == 500_000_000
        # Buffer size argument is converted to megabytes
        assert config.enforce_schema
        assert config.id_type == "INTEGER"
        assert config.skip_invalid_nodes
        assert config.skip_invalid_edges
        assert not config.store_node_identifiers
        assert config.separator == "|"
        assert config.quoting == 0
