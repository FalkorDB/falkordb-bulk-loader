import pytest

from falkordb_bulk_loader.entity_file import Type, typed_prop_to_binary
from falkordb_bulk_loader.exceptions import SchemaError


class TestTypedPropToBinary:
    """Unit tests for typed_prop_to_binary in entity_file."""

    def test_id_integer_valid(self):
        """A parseable integer value for ID_INTEGER succeeds."""
        result = typed_prop_to_binary("42", Type.ID_INTEGER)
        assert result is not None

    def test_id_integer_invalid_raises_schema_error(self):
        """An unparseable value for ID_INTEGER raises SchemaError (regression for silent fallthrough)."""
        with pytest.raises(SchemaError) as exc_info:
            typed_prop_to_binary("not_an_int", Type.ID_INTEGER)
        assert "Could not parse 'not_an_int' as a integer ID" in str(exc_info.value)

    def test_long_invalid_raises_schema_error(self):
        """An unparseable value for LONG raises SchemaError."""
        with pytest.raises(SchemaError) as exc_info:
            typed_prop_to_binary("not_a_long", Type.LONG)
        assert "Could not parse 'not_a_long' as a long" in str(exc_info.value)

    def test_long_valid(self):
        """A parseable integer value for LONG succeeds."""
        result = typed_prop_to_binary("123", Type.LONG)
        assert result is not None
