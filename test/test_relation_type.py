import csv
import os
import unittest

import pytest

from falkordb_bulk_loader.config import Config
from falkordb_bulk_loader.relation_type import RelationType


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


def test_parquet_relation_with_multiple_properties(tmp_path):
    """Verify that Parquet relation files with multiple properties are parsed like CSV."""
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError:
        pytest.skip("pyarrow is not installed; skipping Parquet tests")

    table = pa.table(
        {
            "Start:START_ID(StartNamespace)": [0, 1],
            "End:END_ID(EndNamespace)": [1, 2],
            "weight:DOUBLE": [0.5, 0.8],
        }
    )
    parquet_path = tmp_path / "relations.parquet"
    pq.write_table(table, parquet_path)

    config = Config(enforce_schema=True)
    reltype = RelationType(None, str(parquet_path), "RelationTest", config)

    assert reltype.start_id == 0
    assert reltype.start_namespace == "StartNamespace"
    assert reltype.end_id == 1
    assert reltype.end_namespace == "EndNamespace"
    assert reltype.entity_str == "RelationTest"
    assert reltype.prop_count == 1
    assert reltype.entities_count == 2
    assert reltype.types[0].name == "START_ID" or reltype.types[0].name == "END_ID"
    assert reltype.types[2].name == "DOUBLE"


def test_parquet_relation_schemaless_header(tmp_path):
    """Verify that Parquet relation files work without an explicit schema header."""
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError:
        pytest.skip("pyarrow is not installed; skipping Parquet tests")

    # No :TYPE suffixes here; this should exercise the same default logic as CSV
    # (first column is source ID, second is destination ID, remaining columns
    # are properties).
    table = pa.table(
        {
            "src": [0, 1],
            "dest": [1, 2],
            "weight": [0.5, 0.8],
        }
    )
    parquet_path = tmp_path / "relations_schemaless.parquet"
    pq.write_table(table, parquet_path)

    config = Config()  # enforce_schema=False by default
    reltype = RelationType(None, str(parquet_path), "RelationTest", config)

    assert reltype.start_id == 0
    assert reltype.end_id == 1
    assert reltype.entity_str == "RelationTest"
    # Only the "weight" column should be treated as a property
    assert reltype.prop_count == 1
    assert reltype.entities_count == 2
