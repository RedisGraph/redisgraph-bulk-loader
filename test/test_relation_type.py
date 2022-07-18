import csv
import os
import unittest

from redisgraph_bulk_loader.config import Config
from redisgraph_bulk_loader.relation_type import RelationType


class TestBulkLoader:
    @classmethod
    def teardown_class(cls):
        """Delete temporary files"""
        os.remove("/tmp/relations.tmp")

    def test_process_schemaless_header(self):
        """Verify that a schema-less header is parsed properly."""
        with open("/tmp/relations.tmp", mode="w") as csv_file:
            out = csv.writer(csv_file)
            out.writerow(["START_ID", "END_ID", "property"])
            out.writerow([0, 0, "prop1"])
            out.writerow([1, 1, "prop2"])

        config = Config()
        reltype = RelationType(None, "/tmp/relations.tmp", "RelationTest", config)
        assert reltype.start_id == 0
        assert reltype.end_id == 1
        assert reltype.entity_str == "RelationTest"
        assert reltype.prop_count == 1
        assert reltype.entities_count == 2

    def test_process_header_with_schema(self):
        """Verify that a header with a schema is parsed properly."""
        with open("/tmp/relations.tmp", mode="w") as csv_file:
            out = csv.writer(csv_file)
            out.writerow(
                [
                    "End:END_ID(EndNamespace)",
                    "Start:START_ID(StartNamespace)",
                    "property:STRING",
                ]
            )
            out.writerow([0, 0, "prop1"])
            out.writerow([1, 1, "prop2"])

        config = Config(enforce_schema=True)
        reltype = RelationType(None, "/tmp/relations.tmp", "RelationTest", config)
        assert reltype.start_id == 1
        assert reltype.start_namespace == "StartNamespace"
        assert reltype.end_id == 0
        assert reltype.end_namespace == "EndNamespace"
        assert reltype.entity_str == "RelationTest"
        assert reltype.prop_count == 1
        assert reltype.entities_count == 2
        assert reltype.types[0].name == "END_ID"
        assert reltype.types[1].name == "START_ID"
        assert reltype.types[2].name == "STRING"
