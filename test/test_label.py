import csv
import os
import tempfile
import types
import unittest

from falkordb_bulk_loader.config import Config
from falkordb_bulk_loader.label import Label


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

    def test_update_node_dictionary_returns_false_for_duplicate(self):
        """update_node_dictionary returns False (no sys.exit) when a duplicate
        identifier is seen and skip_invalid_nodes is True."""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False, newline=""
        ) as f:
            tmp_path = f.name
            out = csv.writer(f)
            out.writerow(["_ID", "prop"])
            out.writerow(["a", "val1"])

        try:
            buf = types.SimpleNamespace(nodes={}, top_node_id=0)
            config = Config(store_node_identifiers=True, skip_invalid_nodes=True)
            label = Label(buf, tmp_path, "DupTest", config)

            assert label.update_node_dictionary("a") is True
            assert buf.nodes == {"a": 0}
            assert buf.top_node_id == 1

            # Second call with same identifier must return False without modifying state.
            assert label.update_node_dictionary("a") is False
            assert buf.nodes == {"a": 0}
            assert buf.top_node_id == 1
        finally:
            os.remove(tmp_path)

    def test_process_entities_skips_duplicate_row_when_skip_invalid_nodes(self):
        """process_entities truly skips duplicate rows: node_count and top_node_id
        must both equal 1 when the CSV contains two rows sharing the same identifier."""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False, newline=""
        ) as f:
            tmp_path = f.name
            out = csv.writer(f)
            out.writerow(["_ID", "prop"])
            out.writerow(["dup_id", "first"])
            out.writerow(["dup_id", "second"])  # duplicate — must be skipped

        try:
            buf = types.SimpleNamespace(
                nodes={},
                top_node_id=0,
                buffer_size=0,
                node_count=0,
                labels=[],
            )
            config = Config(
                store_node_identifiers=True,
                skip_invalid_nodes=True,
            )
            label = Label(buf, tmp_path, "DupLabel", config)
            label.process_entities()

            assert (
                buf.top_node_id == 1
            ), "Only one unique identifier should be registered"
            assert (
                buf.node_count == 1
            ), "Only one node should be counted; duplicate must be skipped"
        finally:
            os.remove(tmp_path)
