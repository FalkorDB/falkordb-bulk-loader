"""Unit tests for falkordb_bulk_loader.sources covering edge-case paths."""

import os
import sys
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from falkordb_bulk_loader.sources import ParquetSource, make_source


def test_parquet_source_null_values(tmp_path):
    """ParquetSource should convert None values to empty strings."""
    table = pa.table(
        {
            "name": ["Alice", None, "Charlie"],
            "age": [30, 25, None],
        }
    )
    parquet_path = tmp_path / "nulls.parquet"
    pq.write_table(table, parquet_path)

    src = ParquetSource(str(parquet_path))
    rows = list(src.iter_rows())

    assert rows[0] == ["Alice", "30"]
    assert rows[1] == ["", "25"]
    assert rows[2] == ["Charlie", ""]
    src.close()


def test_parquet_source_close_idempotent(tmp_path):
    """Calling close() twice should not raise."""
    table = pa.table({"x": [1]})
    parquet_path = tmp_path / "data.parquet"
    pq.write_table(table, parquet_path)

    src = ParquetSource(str(parquet_path))
    src.close()
    # _pf is now None; second close should hit the early-return guard
    src.close()


def test_parquet_source_close_oserror(tmp_path):
    """An OSError during close() is silently swallowed."""
    table = pa.table({"x": [1]})
    parquet_path = tmp_path / "data.parquet"
    pq.write_table(table, parquet_path)

    src = ParquetSource(str(parquet_path))
    # Simulate OSError on close
    src._pf.close = MagicMock(side_effect=OSError("disk error"))
    src.close()  # should not raise
    assert src._pf is None


def test_parquet_source_import_error():
    """ParquetSource raises RuntimeError when pyarrow is unavailable."""
    # Temporarily hide pyarrow from the import system
    with patch.dict(sys.modules, {"pyarrow.parquet": None, "pyarrow": None}):
        with pytest.raises(RuntimeError, match="pyarrow is required"):
            ParquetSource("/nonexistent.parquet")


def test_make_source_parquet(tmp_path):
    """make_source returns ParquetSource for .parquet files."""
    table = pa.table({"id": [1, 2]})
    parquet_path = tmp_path / "nodes.parquet"
    pq.write_table(table, parquet_path)

    src = make_source(str(parquet_path), None)
    assert isinstance(src, ParquetSource)
    assert src.header == ["id"]
    assert src.entities_count == 2
    src.close()
