from typing import Self

from source.collection import Collection
from source.db_config import DbConfig
from source.db_ctx import DbCtx
from source.object_id import ObjectId


class DocDbLite:
    def __init__(self, db_dir: str):
        self.db_dir = db_dir
        self.system_db_ctx = DbCtx(DbConfig(self.db_dir, "system.sqlite"))
        self._create_collections_table()
        self._collections_table_name = "collections"

    def _create_collections_table(self):
        self.system_db_ctx.c.execute(
            """
            CREATE TABLE IF NOT EXISTS collections (
                uuid TEXT PRIMARY KEY, -- UUID
                name TEXT NOT NULL
            )
            """
        )

    def add_collection(self: Self, name) -> Collection:
        db_cfg = DbConfig(self.db_dir, f"{name}.sqlite")
        self.system_db_ctx.c.execute(
            f"""
            INSERT INTO {self._collections_table_name} (uuid, name)
            VALUES (?, ?)
            """,
            (str(ObjectId()), name),
        )
        self.system_db_ctx.conn.commit()
        self.system_db_ctx.close()
        return Collection(db_cfg, name)