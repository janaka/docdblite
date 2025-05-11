import os
import sqlite3
from queue import Queue
from sqlite3 import Connection, Cursor

from docdblite.source.db_config import DbConfig


class DbCtx:
    """Database context.

    Creates a reference to a SQLite database and manages a connection pool.
    Thread safe.
    If the database doesn't exist, it will be created.

    Usage:
      ```
        with DbCtx(db_config, database_name) as db:
            # Use db.conn to execute queries
            cursor = db.conn.cursor()
            cursor.execute("SELECT * FROM table_name")
            results = cursor.fetchall()
            # Don't forget to commit or rollback
            db.conn.commit()  # or db.conn.rollback()
            # The connection will be released back to the pool automatically
            # when exiting the with block.
      ```

      Reminder: you can use db.conn.cursor() for more control or conn.execute() for simple one off queries (which creates a temp cursor).
    """

    conn: sqlite3.Connection
    """Connection from pool on entry, returned to pool on exit."""

    def __init__(self, db_config: DbConfig, database_name: str) -> None:
        self.db_cfg: DbConfig = db_config
        db_path = os.path.join(
            self.db_cfg.dir, self._build_database_filename(database_name=database_name)
        )

        # Ensure the directory exists
        os.makedirs(name=self.db_cfg.dir, exist_ok=True)

        self.pool_size = self.db_cfg.connection_pool_size
        self.pool = Queue[Connection](maxsize=self.pool_size)
        # No need for an explicit lock as Queue is already thread-safe

        # Initialize the connection pool
        for _ in range(self.pool_size):
            conn: Connection = sqlite3.connect(
                database=db_path,
                timeout=self.db_cfg.timeout_ms,
                detect_types=sqlite3.PARSE_DECLTYPES,
                cached_statements=self.db_cfg.cached_statements,
            )
            # WAL mode needs to be enabled for each connection
            self._enable_wal_mode(connection=conn)
            self.pool.put(item=conn)

    def _build_database_filename(self, database_name: str) -> str:
        return f"{database_name}.sqlite"

    def _enable_wal_mode(self, connection: Connection) -> None:
        """Enable Write-Ahead Logging (WAL) mode."""
        cursor: Cursor = connection.cursor()
        cursor.execute("PRAGMA journal_mode=WAL;")
        connection.commit()

    def get_connection(self) -> Connection:
        """Get a connection from the pool."""
        return self.pool.get()

    def release_connection(self, connection: Connection) -> None:
        """Release a connection back to the pool.
        Call this immediately after you are done with the connection. i.e. commit or rollback.
        """
        self.pool.put(item=connection)

    def __enter__(self):
        self.conn = self.get_connection()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.release_connection(connection=self.conn)

    def close(self) -> None:
        """Close all connections in the pool."""
        for _ in range(self.pool_size):
            conn: Connection = self.pool.get()
            conn.close()
