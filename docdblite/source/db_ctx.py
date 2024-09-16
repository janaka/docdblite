import os
import sqlite3
from queue import Queue
from threading import Lock

from source.db_config import DbConfig


class DbCtx:
    """Database context.

    Creates a reference to a SQLite database and manages a connection pool.
    If the database doesn't exist, it will be created.

    Usage:
      ```

      ```
    """

    conn: sqlite3.Connection
    """Connection from pool on entry, returned to pool on exit."""

    def __init__(self, db_config: DbConfig, database_name: str):
        self.db_cfg = db_config
        db_path = os.path.join(
            self.db_cfg.dir, self._build_database_filename(database_name)
        )

        # Ensure the directory exists
        os.makedirs(self.db_cfg.dir, exist_ok=True)

        self.pool_size = self.db_cfg.connection_pool_size
        self.pool = Queue(maxsize=self.pool_size)
        self.lock = Lock()

        # Initialize the connection pool
        for _ in range(self.pool_size):
            conn = sqlite3.connect(
                database=db_path,
                timeout=self.db_cfg.timeout_ms,
                detect_types=sqlite3.PARSE_DECLTYPES,
                cached_statements=self.db_cfg.cached_statements,
            )
            conn.execute("PRAGMA journal_mode=WAL;")
            self.pool.put(conn)

        # self.conn = self.get_connection()
        # self.c = self.conn.cursor()

    def _build_database_filename(self, database_name: str) -> str:
        return f"{database_name}.sqlite"

    def _enable_wal_mode(self, connection) -> None:
        """Enable Write-Ahead Logging (WAL) mode."""
        cursor = connection.cursor()
        cursor.execute("PRAGMA journal_mode=WAL;")
        connection.commit()

    def get_connection(self) -> sqlite3.Connection:
        """Get a connection from the pool."""
        with self.lock:
            return self.pool.get()

    def release_connection(self, connection: sqlite3.Connection) -> None:
        """Release a connection back to the pool.
        Call this immediately after you are done with the connection. i.e. commit or rollback.
        """
        with self.lock:
            self.pool.put(connection)

    def __enter__(self):
        self.conn = self.get_connection()
        # self.c = self.conn.cursor()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # self.c.close()
        self.release_connection(self.conn)

    def close(self):
        """Close all connections in the pool."""
        for _ in range(self.pool_size):
            conn = self.pool.get()
            conn.close()
