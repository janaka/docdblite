from typing import Optional, Self

from source.collection import Collection
from source.db_config import DbConfig
from source.db_ctx import DbCtx
from source.object_id import ObjectId


class DocDbLite:
    """This is the DocDb Lite client."""

    def __init__(self, config: Optional[DbConfig] = None):
        self.db_config = config or DbConfig()
        self.system_db_ctx = DbCtx(self.db_config, "system")
        self._create_collections_table()
        self._collections_table_name = "collections"
        self.collections = {}

    def _create_collections_table(self):
        with self.system_db_ctx as sys_db:
            sys_db.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS collections (
                    uuid TEXT PRIMARY KEY, -- UUID
                    name TEXT NOT NULL
                )
                """
            )
            sys_db.conn.commit()

    collections: dict[str, Collection]

    def add_collection(self: Self, name) -> Collection:
        if name in self.collections:
            return self.collections[name]

        # Create a new collection
        with self.system_db_ctx as sys_db:
            sys_db.conn.execute(
                f"""
                INSERT INTO {self._collections_table_name} (uuid, name)
                VALUES (?, ?)
                """,
                (str(ObjectId()), name),
            )
            sys_db.conn.commit()

        self.collections[name] = Collection(self.db_config, name)
        return self.collections[name]