import re
from typing import Optional

from docdblite.source.uuid7 import uuid7

UUID7_REGEX = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-7[0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$"
)

class ObjectId:
    """
    A class representing a UUIDv7 object ID.
    This class is used to generate and validate UUIDv7 identifiers.

    Use as a type.

    The string representation is the UUIDv7 string.

    Attributes:
        value (str): The UUIDv7 string representation.
    """
    __slots__ = ("value",)
    def __init__(self, uuid: Optional[str] = None):
        if uuid is None:
            self.value: str = str(uuid7())

        elif ObjectId.is_uuid7(value=uuid):
            self.value = uuid
        else:
            raise ValueError(f"Invalid UUIDv7: '{uuid}'")

    def __str__(self):
        return self.value

    @staticmethod
    def is_uuid7(value: str) -> bool:
        """Validate if the given string is a valid UUIDv7."""
        return bool(UUID7_REGEX.match(value))
