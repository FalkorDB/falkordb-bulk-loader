import csv
import io
import os
from typing import Iterable, List

from .exceptions import CSVError


class CSVSource:
    """Tabular source backed by a CSV file.

    Exposes a header row, entity count, and an iterator over data rows.
    """

    def __init__(self, filename: str, config):
        self._file = io.open(filename, "rt")
        self.name = os.path.abspath(filename)

        # Initialize CSV reader that ignores leading whitespace in each field
        # and does not modify input quote characters.
        reader = csv.reader(
            self._file,
            delimiter=config.separator,
            skipinitialspace=True,
            quoting=config.quoting,
            escapechar=config.escapechar,
        )

        try:
            header = next(reader)
        except StopIteration:
            raise CSVError(f"{self.name}: Input file is empty")

        self.header: List[str] = header
        # Count remaining rows (data rows only, excluding header).
        self.entities_count = sum(1 for _ in reader)

        # Rewind and recreate reader that skips the header row when iterating.
        self._file.seek(0)
        self._reader = csv.reader(
            self._file,
            delimiter=config.separator,
            skipinitialspace=True,
            quoting=config.quoting,
            escapechar=config.escapechar,
        )
        # Skip header
        next(self._reader, None)

    def iter_rows(self) -> Iterable[List[str]]:
        for row in self._reader:
            yield row

    def close(self) -> None:
        try:
            self._file.close()
        except Exception:
            pass


class ParquetSource:
    """Tabular source backed by a Parquet file.

    Uses pyarrow to read the Parquet file and exposes the same interface
    as CSVSource. Values are converted to strings (or empty string for NULL)
    so the existing type inference logic continues to work unchanged.
    """

    def __init__(self, filename: str, config=None):  # config kept for API symmetry
        try:
            import pyarrow.parquet as pq  # type: ignore
        except ImportError as e:
            raise RuntimeError(
                "pyarrow is required to load Parquet files. "
                "Install it with `pip install pyarrow` or convert your Parquet "
                "files to CSV."
            ) from e

        self.name = os.path.abspath(filename)
        self._table = pq.read_table(filename)
        self.header: List[str] = list(self._table.column_names)
        self.entities_count: int = int(self._table.num_rows)

    def iter_rows(self) -> Iterable[List[str]]:
        # Iterate over record batches to avoid materializing all rows at once.
        # Note: ``RecordBatch.to_pylist()`` returns a list of dicts mapping
        # column names to Python values, not a simple list of values.
        # We must iterate over the columns in ``self.header`` order so that the
        # row layout matches the header just like CSVSource does.
        for batch in self._table.to_batches():
            for row in batch.to_pylist():
                # ``row`` is a dict {column_name: value}; preserve column order
                # according to the header and normalize to strings like CSV.
                values: List[str] = []
                for col in self.header:
                    v = row.get(col)
                    values.append("" if v is None else str(v))
                yield values

    def close(self) -> None:
        # Nothing to close explicitly, but drop reference to table.
        self._table = None


def make_source(filename: str, config):
    """Return an appropriate tabular source for the given file.

    - .parquet -> ParquetSource (requires pyarrow)
    - otherwise -> CSVSource
    """

    ext = os.path.splitext(filename)[1].lower()
    if ext == ".parquet":
        return ParquetSource(filename, config)
    return CSVSource(filename, config)
