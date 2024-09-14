from typing import Optional

from docdblite.source.uuid7 import uuid7


class ObjectId:
    def __init__(self, uuid: Optional[str] = None):
        self.uuid: str = uuid or str(uuid7())

    def __str__(self):
        return self.uuid