import csv
import os
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

    def test_count_entities_multiline_field(self):
        """count_entities must count CSV rows, not physical lines.

        A quoted field containing an embedded newline (RFC 4180) spans two
        physical lines but represents exactly one CSV row/entity.  Using
        raw line-counting inflated the progress-bar total; csv.reader-based
        counting must return the correct entity count.
        """
        test_csv = os.path.join(os.path.dirname(__file__), "multiline_label.tmp")
        try:
            # csv.writer uses QUOTE_MINIMAL by default and will quote the field
            # that contains a newline, producing a valid RFC 4180 file.
            with open(test_csv, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["_ID", "description"])
                writer.writerow([0, "line one\nline two"])  # embedded newline
                writer.writerow([1, "simple"])

            config = Config()  # quoting=csv.QUOTE_MINIMAL by default
            label = Label(None, test_csv, "MultilineTest", config)

            # There are 2 data rows even though the file has 4 physical lines.
            assert label.entities_count == 2
        finally:
            if os.path.exists(test_csv):
                os.remove(test_csv)
