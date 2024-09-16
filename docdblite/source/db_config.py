
from attr import dataclass


@dataclass
class DbConfig:
    dir: str = "../.docdblite"

    MAX_NESTING_LEVELS: int = 100
    """The number of levels of json nodes that can be nested."""

    connection_pool_size: int = 10

    cached_statements: int = 128
    """Number of statements to cache"""

    timeout_ms: int = 5000
    """Busy/connection timeout in milliseconds. Otherwise SQLite will return busy immediately."""
