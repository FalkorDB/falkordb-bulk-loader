"""Unit tests for entity_file helper functions."""

import pytest

from falkordb_bulk_loader.entity_file import (
    array_prop_to_binary,
    inferred_prop_to_binary,
    typed_prop_to_binary,
    Type,
)
from falkordb_bulk_loader.exceptions import SchemaError


class TestArrayPropToBinary:
    """Tests for array_prop_to_binary covering the new SchemaError guard."""

    FORMAT = "=B"

    def test_valid_array(self):
        """A well-formed array literal returns bytes without raising."""
        result = array_prop_to_binary(self.FORMAT, "[1, 2, 3]")
        assert isinstance(result, bytes)
        assert len(result) > 0

    def test_malformed_array_raises_schema_error(self):
        """A syntactically invalid array raises SchemaError (not ValueError/SyntaxError)."""
        with pytest.raises(SchemaError) as exc_info:
            array_prop_to_binary(self.FORMAT, "[not valid python]")
        assert "Could not parse" in str(exc_info.value)
        assert "SyntaxError" in str(exc_info.value) or "ValueError" in str(exc_info.value)

    def test_deeply_nested_array_raises_schema_error(self):
        """A deeply nested literal (RecursionError path) raises SchemaError."""
        deep = "[" * 5000 + "]" * 5000
        with pytest.raises(SchemaError) as exc_info:
            array_prop_to_binary(self.FORMAT, deep)
        assert "Could not parse" in str(exc_info.value)

    def test_schema_error_message_contains_type_name(self):
        """The SchemaError message includes the originating exception type name."""
        with pytest.raises(SchemaError) as exc_info:
            array_prop_to_binary(self.FORMAT, "[1, 2,")  # truncated → SyntaxError
        error_msg = str(exc_info.value)
        assert "SyntaxError" in error_msg or "ValueError" in error_msg


class TestInferredPropToBinary:
    """Tests for inferred_prop_to_binary schemaless fall-through behaviour."""

    def test_valid_array_parsed_as_array(self):
        """A well-formed array literal is returned as an ARRAY binary blob."""
        result = inferred_prop_to_binary("[1, 2]")
        # First byte is the type enum; ARRAY == 5
        assert result[0] == Type.ARRAY.value

    def test_malformed_array_falls_through_to_string(self):
        """A bracket-enclosed value that can't be parsed as an array becomes a string."""
        result = inferred_prop_to_binary("[not valid python]")
        # First byte should be STRING == 3, not ARRAY == 5
        assert result[0] == Type.STRING.value

    def test_plain_string(self):
        """A plain string is returned as a STRING binary blob."""
        result = inferred_prop_to_binary("hello")
        assert result[0] == Type.STRING.value

    def test_integer(self):
        """An integer string is returned as a LONG binary blob."""
        result = inferred_prop_to_binary("42")
        assert result[0] == Type.LONG.value

    def test_float(self):
        """A float string is returned as a DOUBLE binary blob."""
        result = inferred_prop_to_binary("3.14")
        assert result[0] == Type.DOUBLE.value

    def test_bool_true(self):
        """The string 'true' is returned as a BOOL binary blob."""
        result = inferred_prop_to_binary("true")
        assert result[0] == Type.BOOL.value

    def test_bool_false(self):
        """The string 'false' is returned as a BOOL binary blob."""
        result = inferred_prop_to_binary("false")
        assert result[0] == Type.BOOL.value


class TestTypedPropToBinary:
    """Tests for typed_prop_to_binary ensuring SchemaError propagates in schema mode."""

    def test_malformed_array_raises_schema_error_in_typed_mode(self):
        """A malformed array in schema-enforced ARRAY column raises SchemaError."""
        with pytest.raises(SchemaError) as exc_info:
            typed_prop_to_binary("[not valid", Type.ARRAY)
        assert "Could not parse" in str(exc_info.value)

    def test_valid_array_in_typed_mode(self):
        """A valid array in schema-enforced ARRAY column returns bytes."""
        result = typed_prop_to_binary("[1, 2, 3]", Type.ARRAY)
        assert isinstance(result, bytes)
        assert result[0] == Type.ARRAY.value
