"""Connection utilities for handling Redis and Sentinel URLs."""

from urllib.parse import parse_qs, urlparse

import redis
from falkordb import FalkorDB
from redis.sentinel import Sentinel


def parse_sentinel_url(url):
    """
    Parse a sentinel URL and extract connection parameters.

    Expected format:
    redis+sentinel://host1:port1[,host2:port2,...]/service_name[/db][?param=value]

    Args:
        url: Sentinel URL string

    Returns:
        dict: Dictionary containing:
            - sentinels: List of (host, port) tuples
            - service_name: Name of the sentinel service
            - db: Database number (default 0)
            - password: Optional password
            - username: Optional username
            - socket_timeout: Optional socket timeout
            - sentinel_kwargs: Additional sentinel connection parameters
            - connection_kwargs: Additional Redis connection parameters
    """
    parsed = urlparse(url)

    if parsed.scheme not in ["redis+sentinel", "rediss+sentinel"]:
        raise ValueError(
            f"Invalid sentinel URL scheme: {parsed.scheme}. "
            "Expected 'redis+sentinel://' or 'rediss+sentinel://'"
        )

    # Parse sentinel hosts
    sentinels = []
    if parsed.netloc:
        # Handle authentication in netloc
        netloc = parsed.netloc
        username = None
        password = None

        # Extract credentials if present
        if "@" in netloc:
            auth_part, hosts_part = netloc.rsplit("@", 1)
            if ":" in auth_part:
                username, password = auth_part.split(":", 1)
            else:
                password = auth_part
        else:
            hosts_part = netloc

        # Parse comma-separated host:port pairs
        for host_port in hosts_part.split(","):
            host_port = host_port.strip()
            if ":" in host_port:
                host, port = host_port.rsplit(":", 1)
                sentinels.append((host, int(port)))
            else:
                sentinels.append((host_port, 26379))  # Default sentinel port

    if not sentinels:
        raise ValueError("No sentinel hosts specified in URL")

    # Parse path to get service name and db
    path_parts = [p for p in parsed.path.split("/") if p]
    if not path_parts:
        raise ValueError("Service name must be specified in sentinel URL path")

    service_name = path_parts[0]
    db = int(path_parts[1]) if len(path_parts) > 1 else 0

    # Parse query parameters
    params = parse_qs(parsed.query)

    # Extract common parameters
    socket_timeout = (
        float(params.get("socket_timeout", [None])[0])
        if "socket_timeout" in params
        else None
    )

    # Override password from query params if present
    if "password" in params:
        password = params["password"][0]

    # Build kwargs dictionaries
    sentinel_kwargs = {}
    connection_kwargs = {
        "db": db,
        "decode_responses": False,
    }

    if password:
        connection_kwargs["password"] = password
    if username:
        connection_kwargs["username"] = username
    if socket_timeout:
        sentinel_kwargs["socket_timeout"] = socket_timeout
        connection_kwargs["socket_timeout"] = socket_timeout

    # Use SSL if scheme is rediss+sentinel
    if parsed.scheme == "rediss+sentinel":
        connection_kwargs["ssl"] = True

    return {
        "sentinels": sentinels,
        "service_name": service_name,
        "db": db,
        "password": password,
        "username": username,
        "socket_timeout": socket_timeout,
        "sentinel_kwargs": sentinel_kwargs,
        "connection_kwargs": connection_kwargs,
    }


def create_redis_connection(url):
    """
    Create a Redis connection from a URL.

    Supports both regular Redis URLs and Sentinel URLs.

    Args:
        url: Redis connection URL (redis://, rediss://, unix://, or redis+sentinel://)

    Returns:
        redis.Redis: Redis client instance
    """
    if url.startswith("redis+sentinel://") or url.startswith("rediss+sentinel://"):
        # Handle sentinel URL
        config = parse_sentinel_url(url)
        sentinel = Sentinel(
            config["sentinels"],
            sentinel_kwargs=config["sentinel_kwargs"],
            **config["connection_kwargs"],
        )
        # Get master connection
        return sentinel.master_for(config["service_name"], db=config["db"])
    else:
        # Handle regular Redis URL
        return redis.from_url(url)


def create_falkordb_client(url):
    """
    Create a FalkorDB client from a URL.

    Supports both regular Redis URLs and Sentinel URLs.

    Args:
        url: Redis connection URL (redis://, rediss://, unix://, or redis+sentinel://)

    Returns:
        FalkorDB: FalkorDB client instance
    """
    if url.startswith("redis+sentinel://") or url.startswith("rediss+sentinel://"):
        # Handle sentinel URL
        config = parse_sentinel_url(url)
        sentinel = Sentinel(
            config["sentinels"],
            sentinel_kwargs=config["sentinel_kwargs"],
            **config["connection_kwargs"],
        )
        # Get master connection for FalkorDB
        redis_client = sentinel.master_for(config["service_name"], db=config["db"])
        # Create FalkorDB instance with the sentinel-based Redis connection
        return FalkorDB(connection=redis_client)
    else:
        # Handle regular Redis URL
        return FalkorDB.from_url(url)
