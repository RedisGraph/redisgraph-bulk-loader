import unittest
from redisgraph_bulk_loader.config import Config, Config_Set


class TestBulkLoader(unittest.TestCase):
    def test01_default_values(self):
        """Verify the default values in the Config class."""

        self.assertEqual(Config.max_token_count, 1024 * 1023)
        self.assertEqual(Config.max_buffer_size, 2_048_000_000)
        self.assertEqual(Config.max_token_size, 512_000_000)
        self.assertEqual(Config.enforce_schema, False)
        self.assertEqual(Config.skip_invalid_nodes, False)
        self.assertEqual(Config.skip_invalid_edges, False)
        self.assertEqual(Config.store_node_identifiers, False)
        self.assertEqual(Config.separator, ',')
        self.assertEqual(Config.quoting, 3)

    def test02_config_set(self):
        """Verify that Config_set updates Config class values accordingly."""
        Config_Set(max_token_count=10, max_buffer_size=100, max_token_size=200, enforce_schema=True, skip_invalid_nodes=True, skip_invalid_edges=True, separator='|', quoting=0)
        self.assertEqual(Config.max_token_count, 10)
        self.assertEqual(Config.max_token_size, 200_000_000) # Max token size argument is converted to megabytes
        self.assertEqual(Config.max_buffer_size, 100_000_000) # Buffer size argument is converted to megabytes

        self.assertEqual(Config.enforce_schema, True)
        self.assertEqual(Config.skip_invalid_nodes, True)
        self.assertEqual(Config.skip_invalid_edges, True)
        self.assertEqual(Config.store_node_identifiers, False)
        self.assertEqual(Config.separator, '|')
        self.assertEqual(Config.quoting, 0)
