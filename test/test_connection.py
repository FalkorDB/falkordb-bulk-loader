"""Tests for connection utilities."""

import pytest

from falkordb_bulk_loader.connection import (
    create_falkordb_client,
    create_redis_connection,
    parse_sentinel_url,
)


class TestSentinelURLParsing:
    """Test sentinel URL parsing functionality."""

    def test_basic_sentinel_url(self):
        """Test parsing a basic sentinel URL."""
        url = "redis+sentinel://localhost:26379/mymaster/0"
        config = parse_sentinel_url(url)

        assert config["sentinels"] == [("localhost", 26379)]
        assert config["service_name"] == "mymaster"
        assert config["db"] == 0
        assert config["password"] is None
        assert config["username"] is None

    def test_sentinel_url_multiple_hosts(self):
        """Test parsing a sentinel URL with multiple hosts."""
        url = "redis+sentinel://host1:26379,host2:26380,host3:26381/mymaster/1"
        config = parse_sentinel_url(url)

        assert config["sentinels"] == [
            ("host1", 26379),
            ("host2", 26380),
            ("host3", 26381),
        ]
        assert config["service_name"] == "mymaster"
        assert config["db"] == 1

    def test_sentinel_url_with_password(self):
        """Test parsing a sentinel URL with password."""
        url = "redis+sentinel://:mypassword@localhost:26379/mymaster/0"
        config = parse_sentinel_url(url)

        assert config["sentinels"] == [("localhost", 26379)]
        assert config["service_name"] == "mymaster"
        assert config["password"] == "mypassword"

    def test_sentinel_url_with_username_and_password(self):
        """Test parsing a sentinel URL with username and password."""
        url = "redis+sentinel://user:pass@localhost:26379/mymaster/0"
        config = parse_sentinel_url(url)

        assert config["sentinels"] == [("localhost", 26379)]
        assert config["service_name"] == "mymaster"
        assert config["username"] == "user"
        assert config["password"] == "pass"

    def test_sentinel_url_with_query_params(self):
        """Test parsing a sentinel URL with query parameters."""
        url = "redis+sentinel://localhost:26379/mymaster/0?socket_timeout=5.0"
        config = parse_sentinel_url(url)

        assert config["sentinels"] == [("localhost", 26379)]
        assert config["service_name"] == "mymaster"
        assert config["socket_timeout"] == 5.0

    def test_sentinel_url_default_port(self):
        """Test parsing a sentinel URL without explicit port."""
        url = "redis+sentinel://localhost/mymaster/0"
        config = parse_sentinel_url(url)

        assert config["sentinels"] == [("localhost", 26379)]
        assert config["service_name"] == "mymaster"

    def test_sentinel_url_default_db(self):
        """Test parsing a sentinel URL without explicit db."""
        url = "redis+sentinel://localhost:26379/mymaster"
        config = parse_sentinel_url(url)

        assert config["sentinels"] == [("localhost", 26379)]
        assert config["service_name"] == "mymaster"
        assert config["db"] == 0

    def test_sentinels_ssl_scheme(self):
        """Test parsing a secure sentinel URL."""
        url = "rediss+sentinel://localhost:26379/mymaster/0"
        config = parse_sentinel_url(url)

        assert config["sentinels"] == [("localhost", 26379)]
        assert config["service_name"] == "mymaster"
        assert config["connection_kwargs"]["ssl"] is True

    def test_invalid_scheme(self):
        """Test that invalid schemes raise an error."""
        url = "http://localhost:26379/mymaster/0"
        with pytest.raises(ValueError, match="Invalid sentinel URL scheme"):
            parse_sentinel_url(url)

    def test_missing_service_name(self):
        """Test that missing service name raises an error."""
        url = "redis+sentinel://localhost:26379/"
        with pytest.raises(ValueError, match="Service name must be specified"):
            parse_sentinel_url(url)

    def test_no_hosts(self):
        """Test that missing hosts raise an error."""
        url = "redis+sentinel:///mymaster/0"
        with pytest.raises(ValueError, match="No sentinel hosts specified"):
            parse_sentinel_url(url)

    def test_invalid_port_number(self):
        """Test that invalid port numbers raise an error."""
        url = "redis+sentinel://localhost:invalid/mymaster/0"
        with pytest.raises(ValueError, match="Invalid port number"):
            parse_sentinel_url(url)

    def test_invalid_db_number(self):
        """Test that invalid database numbers raise an error."""
        url = "redis+sentinel://localhost:26379/mymaster/invalid"
        with pytest.raises(ValueError, match="Invalid database number"):
            parse_sentinel_url(url)


class TestConnectionCreation:
    """Test connection creation with regular and sentinel URLs."""

    def test_regular_url_detection(self):
        """Test that regular Redis URLs are handled correctly."""
        # This test just verifies that regular URLs don't crash
        # We can't actually connect without a Redis server
        url = "redis://localhost:6379"
        try:
            # Just verify it doesn't raise an exception during creation attempt
            client = create_redis_connection(url)
        except Exception as e:
            # Connection errors are expected without a server
            # But parsing/creation errors are not
            if "Connection" not in str(e) and "connection" not in str(e):
                raise

    def test_falkordb_regular_url(self):
        """Test FalkorDB client creation with regular URL."""
        url = "redis://localhost:6379"
        try:
            client = create_falkordb_client(url)
        except Exception as e:
            # Connection errors are expected without a server
            if "Connection" not in str(e) and "connection" not in str(e):
                raise
