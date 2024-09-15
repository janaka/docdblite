from attr import dataclass


@dataclass
class DbConfig:
    dir: str
    filename: str
    MAX_NESTING_LEVELS: int = 100
    """The number of levels of json nodes that can be nested."""
    connection_pool_size: int = 10
