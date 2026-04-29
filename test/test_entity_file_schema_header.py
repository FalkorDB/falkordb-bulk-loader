"""Tests for schema-mode header validation in EntityFile."""

import csv
import tempfile
import os

import pytest

from falkordb_bulk_loader.config import Config
from falkordb_bulk_loader.exceptions import CSVError
from falkordb_bulk_loader.label import Label


def _write_csv(path: str, header: list[str]) -> None:
    with open(path, mode="w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerow(["val"] * len(header))


class TestSchemaHeaderValidation:
    """Tests for convert_header_with_schema error messages."""

    def test_too_many_colons_reports_correct_count(self, tmp_path):
        """Field with multiple colons should report the colon count, not string length."""
        csv_file = tmp_path / "labels.csv"
        _write_csv(str(csv_file), ["a:B:C"])
        config = Config(enforce_schema=True)
        with pytest.raises(CSVError) as exc_info:
            Label(None, str(csv_file), "L", config)
        assert "2 colons" in str(
            exc_info.value
        ), "Error message should report 2 colons for field 'a:B:C'"
        assert "a:B:C" in str(exc_info.value)

    def test_too_many_colons_single_extra(self, tmp_path):
        """Field 'x:Y:Z:W' should report 3 colons."""
        csv_file = tmp_path / "labels.csv"
        _write_csv(str(csv_file), ["x:Y:Z:W"])
        config = Config(enforce_schema=True)
        with pytest.raises(CSVError) as exc_info:
            Label(None, str(csv_file), "L", config)
        assert "3 colons" in str(exc_info.value)

    def test_missing_colon_raises_csv_error(self, tmp_path):
        """A field with no colon separator should raise CSVError."""
        csv_file = tmp_path / "labels.csv"
        _write_csv(str(csv_file), ["property"])
        config = Config(enforce_schema=True)
        with pytest.raises(CSVError) as exc_info:
            Label(None, str(csv_file), "L", config)
        assert "property" in str(exc_info.value)
        assert "colon" in str(exc_info.value).lower()

    def test_valid_schema_header_parses_correctly(self, tmp_path):
        """A well-formed schema header should parse without errors."""
        csv_file = tmp_path / "labels.csv"
        _write_csv(str(csv_file), ["id:ID", "name:STRING"])
        config = Config(enforce_schema=True, store_node_identifiers=True)
        label = Label(None, str(csv_file), "L", config)
        assert label.types[0].name == "ID_STRING"
        assert label.types[1].name == "STRING"
