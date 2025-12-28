import csv
import os
import unittest

import pytest

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


def test_parquet_label_with_multiple_properties(tmp_path):
    """Verify that Parquet label files with multiple properties are parsed like CSV."""
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError:
        pytest.skip("pyarrow is not installed; skipping Parquet tests")

    table = pa.table(
        {
            "id:ID(IDNamespace)": [0, 1],
            "name:STRING": ["Jeff", "Jane"],
            "age:INT": [30, 40],
        }
    )
    parquet_path = tmp_path / "labels.parquet"
    pq.write_table(table, parquet_path)

    config = Config(enforce_schema=True, store_node_identifiers=True)
    label = Label(None, str(parquet_path), "LabelTest", config)

    # Same behavior as CSV header parsing
    assert label.column_names == ["id", "name", "age"]
    assert label.column_count == 3
    assert label.id_namespace == "IDNamespace"
    assert label.prop_count == 3  # id is exposed as a property when it has a name
    assert label.entities_count == 2
    assert label.types[0].name == "ID_STRING"
    assert label.types[1].name == "STRING"
    assert label.types[2].name == "LONG"
