# -*- coding: utf-8 -*-

import csv
import os
import unittest

import redis
from click.testing import CliRunner
from redis import Redis

from redisgraph_bulk_loader.bulk_update import bulk_update


class TestBulkUpdate:

    redis_con = redis.Redis(decode_responses=True)

    @classmethod
    def setup_class(cls):
        cls.redis_con.flushall()

    @classmethod
    def teardown_class(cls):
        """Delete temporary files"""
        os.unlink("/tmp/csv.tmp")
        cls.redis_con.flushall()

    def test_simple_updates(self):
        """Validate that bulk updates work on an empty graph."""
        graphname = "tmpgraph1"
        # Write temporary files
        with open("/tmp/csv.tmp", mode="w") as csv_file:
            out = csv.writer(csv_file)
            out.writerow(["id", "name"])
            out.writerow([0, "a"])
            out.writerow([5, "b"])
            out.writerow([3, "c"])

        runner = CliRunner()
        res = runner.invoke(
            bulk_update,
            [
                "--csv",
                "/tmp/csv.tmp",
                "--query",
                "CREATE (:L {id: row[0], name: row[1]})",
                graphname,
            ],
            catch_exceptions=False,
        )

        assert res.exit_code == 0
        assert "Labels added: 1" in res.output
        assert "Nodes created: 3" in res.output
        assert "Properties set: 6" in res.output

        tmp_graph = self.redis_con.graph(graphname)
        query_result = tmp_graph.query("MATCH (a) RETURN a.id, a.name ORDER BY a.id")

        # Validate that the expected results are all present in the graph
        expected_result = [[0, "a"], [3, "c"], [5, "b"]]
        assert query_result.result_set == expected_result

        # Attempt to re-insert the entities using MERGE.
        res = runner.invoke(
            bulk_update,
            [
                "--csv",
                "/tmp/csv.tmp",
                "--query",
                "MERGE (:L {id: row[0], name: row[1]})",
                graphname,
            ],
            catch_exceptions=False,
        )

        # No new entities should be created.
        assert res.exit_code == 0
        assert "Labels added" not in res.output
        assert "Nodes created" not in res.output
        assert "Properties set" not in res.output

    def test_traversal_updates(self):
        """Validate that bulk updates can create edges and perform traversals."""
        graphname = "tmpgraph1"
        # Write temporary files
        with open("/tmp/csv.tmp", mode="w") as csv_file:
            out = csv.writer(csv_file)
            out.writerow(["src", "dest_id", "name"])
            out.writerow([0, 1, "a2"])
            out.writerow([5, 2, "b2"])
            out.writerow([3, 4, "c2"])

        # Create a graph of the form:
        # (a)-->(b)-->(c), (a)-->(c)
        runner = CliRunner()
        res = runner.invoke(
            bulk_update,
            [
                "--csv",
                "/tmp/csv.tmp",
                "--query",
                "MATCH (src {id: row[0]}) CREATE (src)-[:R]->(dest:L {id: row[1], name: row[2]})",
                graphname,
            ],
            catch_exceptions=False,
        )

        assert res.exit_code == 0
        assert "Nodes created: 3" in res.output
        assert "Relationships created: 3" in res.output
        assert "Properties set: 6" in res.output

        tmp_graph = self.redis_con.graph(graphname)
        query_result = tmp_graph.query(
            "MATCH (a)-[:R]->(b) RETURN a.name, b.name ORDER BY a.name, b.name"
        )

        # Validate that the expected results are all present in the graph
        expected_result = [["a", "a2"], ["b", "b2"], ["c", "c2"]]
        assert query_result.result_set == expected_result

    def test_datatypes(self):
        """Validate that all RedisGraph datatypes are supported by the bulk updater."""
        graphname = "tmpgraph2"
        # Write temporary files
        with open("/tmp/csv.tmp", mode="w") as csv_file:
            out = csv.writer(csv_file)
            out.writerow([0, 1.5, "true", "string", "[1, 'nested_str']"])

        runner = CliRunner()
        res = runner.invoke(
            bulk_update,
            [
                "--csv",
                "/tmp/csv.tmp",
                "--query",
                "CREATE (a:L) SET a.intval = row[0], a.doubleval = row[1], a.boolval = row[2], a.stringval = row[3], a.arrayval = row[4]",
                "--no-header",
                graphname,
            ],
            catch_exceptions=False,
        )

        assert res.exit_code == 0
        assert "Nodes created: 1" in res.output
        assert "Properties set: 5" in res.output

        tmp_graph = self.redis_con.graph(graphname)
        query_result = tmp_graph.query(
            "MATCH (a) RETURN a.intval, a.doubleval, a.boolval, a.stringval, a.arrayval"
        )

        # Validate that the expected results are all present in the graph
        expected_result = [[0, 1.5, True, "string", "[1,'nested_str']"]]
        assert query_result.result_set == expected_result

    def test_custom_delimiter(self):
        """Validate that non-comma delimiters produce the correct results."""
        graphname = "tmpgraph3"
        # Write temporary files
        with open("/tmp/csv.tmp", mode="w") as csv_file:
            out = csv.writer(csv_file, delimiter="|")
            out.writerow(["id", "name"])
            out.writerow([0, "a"])
            out.writerow([5, "b"])
            out.writerow([3, "c"])

        runner = CliRunner()
        res = runner.invoke(
            bulk_update,
            [
                "--csv",
                "/tmp/csv.tmp",
                "--query",
                "CREATE (:L {id: row[0], name: row[1]})",
                "--separator",
                "|",
                graphname,
            ],
            catch_exceptions=False,
        )

        assert res.exit_code == 0
        assert "Labels added: 1" in res.output
        assert "Nodes created: 3" in res.output
        assert "Properties set: 6" in res.output

        tmp_graph = self.redis_con.graph(graphname)
        query_result = tmp_graph.query("MATCH (a) RETURN a.id, a.name ORDER BY a.id")

        # Validate that the expected results are all present in the graph
        expected_result = [[0, "a"], [3, "c"], [5, "b"]]
        assert query_result.result_set == expected_result

        # Attempt to re-insert the entities using MERGE.
        res = runner.invoke(
            bulk_update,
            [
                "--csv",
                "/tmp/csv.tmp",
                "--query",
                "MERGE (:L {id: row[0], name: row[1]})",
                "--separator",
                "|",
                graphname,
            ],
            catch_exceptions=False,
        )

        # No new entities should be created.
        assert res.exit_code == 0
        assert "Labels added" not in res.output
        assert "Nodes created" not in res.output
        assert "Properties set" not in res.output

    def test_custom_variable_name(self):
        """Validate that the user can specify the name of the 'row' query variable."""
        graphname = "variable_name"
        runner = CliRunner()

        csv_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "../example/"
        )
        person_file = os.path.join(csv_path, "Person.csv")
        # Build the social graph again with a max token count of 1.
        res = runner.invoke(
            bulk_update,
            [
                "--csv",
                person_file,
                "--query",
                "CREATE (p:Person) SET p.name = line[0], p.age = line[1], p.gender = line[2], p.status = line[3]",
                "--variable-name",
                "line",
                graphname,
            ],
            catch_exceptions=False,
        )

        assert res.exit_code == 0
        assert "Labels added: 1" in res.output
        assert "Nodes created: 14" in res.output
        assert "Properties set: 56" in res.output

        tmp_graph = self.redis_con.graph(graphname)

        # Validate that the expected results are all present in the graph
        query_result = tmp_graph.query(
            "MATCH (p:Person) RETURN p.name, p.age, p.gender, p.status ORDER BY p.name"
        )
        expected_result = [
            ["Ailon Velger", 32, "male", "married"],
            ["Alon Fital", 32, "male", "married"],
            ["Boaz Arad", 31, "male", "married"],
            ["Gal Derriere", 26, "male", "single"],
            ["Jane Chernomorin", 31, "female", "married"],
            ["Lucy Yanfital", 30, "female", "married"],
            ["Mor Yesharim", 31, "female", "married"],
            ["Noam Nativ", 34, "male", "single"],
            ["Omri Traub", 33, "male", "single"],
            ["Ori Laslo", 32, "male", "married"],
            ["Roi Lipman", 32, "male", "married"],
            ["Shelly Laslo Rooz", 31, "female", "married"],
            ["Tal Doron", 32, "male", "single"],
            ["Valerie Abigail Arad", 31, "female", "married"],
        ]
        assert query_result.result_set == expected_result

    def test_no_header(self):
        """Validate that the '--no-header' option works properly."""
        graphname = "tmpgraph4"
        # Write temporary files
        with open("/tmp/csv.tmp", mode="w") as csv_file:
            out = csv.writer(csv_file)
            out.writerow([0, "a"])
            out.writerow([5, "b"])
            out.writerow([3, "c"])

        runner = CliRunner()
        res = runner.invoke(
            bulk_update,
            [
                "--csv",
                "/tmp/csv.tmp",
                "--query",
                "CREATE (:L {id: row[0], name: row[1]})",
                "--no-header",
                graphname,
            ],
            catch_exceptions=False,
        )

        assert res.exit_code == 0
        assert "Labels added: 1" in res.output
        assert "Nodes created: 3" in res.output
        assert "Properties set: 6" in res.output

        tmp_graph = self.redis_con.graph(graphname)
        query_result = tmp_graph.query("MATCH (a) RETURN a.id, a.name ORDER BY a.id")

        # Validate that the expected results are all present in the graph
        expected_result = [[0, "a"], [3, "c"], [5, "b"]]
        assert query_result.result_set == expected_result

    def test_batched_update(self):
        """Validate that updates performed over multiple batches produce the correct results."""
        graphname = "batched_update"

        prop_str = "Property value to be repeated 100 thousand times generating a multi-megabyte CSV"
        # Write temporary files
        with open("/tmp/csv.tmp", mode="w") as csv_file:
            out = csv.writer(csv_file)
            for i in range(100_000):
                out.writerow([prop_str])

        runner = CliRunner()
        res = runner.invoke(
            bulk_update,
            [
                "--csv",
                "/tmp/csv.tmp",
                "--query",
                "CREATE (:L {prop: row[0]})",
                "--no-header",
                "--max-token-size",
                1,
                graphname,
            ],
            catch_exceptions=False,
        )

        assert res.exit_code == 0
        assert "Labels added: 1" in res.output
        assert "Nodes created: 100000" in res.output
        assert "Properties set: 100000" in res.output

        tmp_graph = self.redis_con.graph(graphname)
        query_result = tmp_graph.query("MATCH (a) RETURN DISTINCT a.prop")

        # Validate that the expected results are all present in the graph
        expected_result = [[prop_str]]
        assert query_result.result_set == expected_result

    def test_runtime_error(self):
        """Validate that run-time errors are captured by the bulk updater."""
        graphname = "tmpgraph5"

        # Write temporary files
        with open("/tmp/csv.tmp", mode="w") as csv_file:
            out = csv.writer(csv_file)
            out.writerow(["a"])
        runner = CliRunner()
        res = runner.invoke(
            bulk_update,
            [
                "--csv",
                "/tmp/csv.tmp",
                "--query",
                "MERGE (:L {val: NULL})",
                "--no-header",
                graphname,
            ],
        )

        assert res.exit_code != 0
        assert "Cannot merge node" in str(res.exception)

    def test_compile_time_error(self):
        """Validate that malformed queries trigger an early exit from the bulk updater."""
        graphname = "tmpgraph5"
        runner = CliRunner()
        res = runner.invoke(
            bulk_update,
            [
                "--csv",
                "/tmp/csv.tmp",
                "--query",
                "CREATE (:L {val: row[0], val2: undefined_identifier})",
                "--no-header",
                graphname,
            ],
        )

        assert res.exit_code != 0
        assert "undefined_identifier not defined" in str(res.exception)

    def test_invalid_inputs(self):
        """Validate that the bulk updater handles invalid inputs incorrectly."""
        graphname = "tmpgraph6"

        # Attempt to insert a non-existent CSV file.
        runner = CliRunner()
        res = runner.invoke(
            bulk_update,
            [
                "--csv",
                "/tmp/fake_file.csv",
                "--query",
                "MERGE (:L {val: NULL})",
                graphname,
            ],
        )

        assert res.exit_code != 0
        assert "No such file" in str(res.exception)
