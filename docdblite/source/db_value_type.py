from enum import Enum


class DbValueType(Enum):
    # avoid 0 and 1 because that's bool in sqlite
    NULL = 5
    OBJECT = 10
    ARRAY = 15
    STRING = 20
    INTEGER = 25
    FLOAT = 30
    BOOLEAN = 35
    DATETIME = 40