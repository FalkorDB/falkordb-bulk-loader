import csv
import io
import os
from typing import Iterable, List

from .exceptions import CSVError


class CSVSource:
    """Row-oriented tabular source backed by a CSV file.

    This class exposes a header row, an entity (row) count, and an iterator
    over data rows. It is intentionally minimal so that higher-level
    components (labels, relation types) do not need to know whether the
    underlying storage is CSV or something else.
    """

    def __init__(self, filename: str, config):
        """Open ``filename`` and prepare a CSV reader.

        The first row is treated as the header; all subsequent rows are
        counted and yielded by :meth:`iter_rows`.
        """
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
        """Yield each data row as a list of strings.

        The row layout matches ``self.header``.
        """
        for row in self._reader:
            yield row

    def close(self) -> None:
        """Close the underlying CSV file.

        Any ``OSError`` during close is ignored, as it is unlikely to affect
        correctness in this batch-oriented CLI tool.
        """
        try:
            self._file.close()
        except OSError:
            # Non-fatal; we are typically at process teardown.
            pass


class ParquetSource:
        """Row-oriented tabular source backed by a Parquet file.

        Uses :mod:`pyarrow.parquet` to expose the same interface as
        :class:`CSVSource`. Values are converted to strings (or the empty
        string for NULL) so that the existing type inference logic continues
        to work unchanged.
        """

        def __init__(self, filename: str, config=None):  # config kept for API symmetry
            """Open ``filename`` as a Parquet dataset.

            The header and entity count are derived from file metadata so we
            do not need to materialize the entire file in memory at once.
            """
            try:
                import pyarrow.parquet as pq  # type: ignore
            except ImportError as e:
                raise RuntimeError(
                    "pyarrow is required to load Parquet files. "
                    "Install it with `pip install pyarrow` or convert your Parquet "
                    "files to CSV."
                ) from e

            self.name = os.path.abspath(filename)
            # Use ParquetFile to avoid reading the entire dataset eagerly.
            self._pf = pq.ParquetFile(filename)

            # Column names and row count come from the schema/metadata.
            schema = self._pf.schema_arrow
            self.header = list(schema.names)
            self.entities_count = int(self._pf.metadata.num_rows)

        def iter_rows(self) -> Iterable[List[str]]:
            """Yield each row as a list of strings in ``self.header`` order.

            Iterates over record batches for each row group to keep memory
            usage bounded for large Parquet files.
            """
            # ``iter_batches`` yields RecordBatch instances without
            # materializing the whole table at once.
            for batch in self._pf.iter_batches():
                # ``to_pylist`` on a RecordBatch returns a list of dicts
                # mapping column names to Python values.
                for row in batch.to_pylist():
                    values: List[str] = []
                    for col in self.header:
                        v = row.get(col)
                        values.append("" if v is None else str(v))
                    yield values

        def close(self) -> None:
            """Close the underlying Parquet file object."""
            try:
                self._pf.close()
            except (OSError, AttributeError):
                pass
            self._pf = None


def make_source(filename: str, config):
    """Return an appropriate tabular source for the given file.

    - ``.parquet`` files use :class:`ParquetSource` (requires ``pyarrow``).
    - All other files are treated as CSV via :class:`CSVSource`.
    """

    ext = os.path.splitext(filename)[1].lower()
    if ext == ".parquet":
        return ParquetSource(filename, config)
    return CSVSource(filename, config)
