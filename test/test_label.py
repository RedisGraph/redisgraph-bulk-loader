import csv
import os
import unittest

from redisgraph_bulk_loader.config import Config
from redisgraph_bulk_loader.label import Label


class TestBulkLoader:
    @classmethod
    def teardown_class(cls):
        """Delete temporary files"""
        os.remove("/tmp/labels.tmp")

    def test_process_schemaless_header(self):
        """Verify that a schema-less header is parsed properly."""
        with open("/tmp/labels.tmp", mode="w") as csv_file:
            out = csv.writer(csv_file)
            out.writerow(["_ID", "prop"])
            out.writerow([0, "prop1"])
            out.writerow([1, "prop2"])

        config = Config()
        label = Label(None, "/tmp/labels.tmp", "LabelTest", config)

        # The '_ID' column will not be stored, as the underscore indicates a private identifier.
        assert label.column_names == [None, "prop"]
        assert label.column_count == 2
        assert label.id == 0
        assert label.entity_str == "LabelTest"
        assert label.prop_count == 1
        assert label.entities_count == 2

    def test_process_header_with_schema(self):
        """Verify that a header with a schema is parsed properly."""
        with open("/tmp/labels.tmp", mode="w") as csv_file:
            out = csv.writer(csv_file)
            out.writerow(["id:ID(IDNamespace)", "property:STRING"])
            out.writerow([0, 0, "prop1"])
            out.writerow([1, 1, "prop2"])

        config = Config(enforce_schema=True, store_node_identifiers=True)
        label = Label(None, "/tmp/labels.tmp", "LabelTest", config)
        assert label.column_names == ["id", "property"]
        assert label.column_count == 2
        assert label.id_namespace == "IDNamespace"
        assert label.entity_str == "LabelTest"
        assert label.prop_count == 2
        assert label.entities_count == 2
        assert label.types[0].name == "ID_STRING"
        assert label.types[1].name == "STRING"
