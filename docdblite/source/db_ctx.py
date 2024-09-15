import os
import sqlite3
from queue import Queue
from threading import Lock

from source.db_config import DbConfig


class DbCtx:
    # TODO: set WAL mode and other config here
    def __init__(self, db_config: DbConfig):
        self.db_cfg = db_config
        db_path = os.path.join(self.db_cfg.dir, self.db_cfg.filename)
        # Ensure the directory exists
        os.makedirs(self.db_cfg.dir, exist_ok=True)

        self.pool_size = self.db_cfg.connection_pool_size
        self.pool = Queue(maxsize=self.pool_size)
        self.lock = Lock()

        # Initialize the connection pool
        for _ in range(self.pool_size):
            conn = sqlite3.connect(
                database=db_path, detect_types=sqlite3.PARSE_DECLTYPES
            )
            self._enable_wal_mode(conn)
            self.pool.put(conn)

        self.conn = self.get_connection()
        self.c = self.conn.cursor()
        # self.conn = sqlite3.connect(
        #     database=db_path, detect_types=sqlite3.PARSE_DECLTYPES
        # )  # need to do connection pooling here
        # self.c = self.conn.cursor()
        # self._enable_wal_mode()
        # self.conn.commit()

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
        """Release a connection back to the pool."""
        with self.lock:
            self.pool.put(connection)

    def __enter__(self):
        self.conn = self.get_connection()
        self.c = self.conn.cursor()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.c.close()
        self.release_connection(self.conn)

    def close(self):
        """Close all connections in the pool."""
        self.release_connection(self.conn)
