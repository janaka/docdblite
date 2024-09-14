import datetime
import json
import os
import sqlite3
from enum import Enum
from typing import Any, Mapping, Optional, Self, Union

from attr import dataclass

from docdblite.source.object_id import ObjectId


class JsonValueType(Enum):
    # avoid 0 and 1 because that's bool in sqlite
    NULL = 5
    OBJECT = 10
    ARRAY = 15
    STRING = 20
    INTEGER = 25
    FLOAT = 30
    BOOLEAN = 35
    DATETIME = 40


@dataclass
class DbConfig:
    dir: str
    filename: str
    MAX_NESTING_LEVELS: int = 100
    """The number of levels of json nodes that can be nested."""


class DbCtx:
    def __init__(self, db_config: DbConfig):
        self.db_cfg = db_config
        db_path = os.path.join(self.db_cfg.dir, self.db_cfg.filename)
        # Ensure the directory exists
        os.makedirs(self.db_cfg.dir, exist_ok=True)
        self.conn = sqlite3.connect(
            database=db_path,
        )  # need to do connection pooling here
        self.c = self.conn.cursor()
        self.conn.commit()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        self.conn.close()


class Collection:
    def __init__(self, db_config: DbConfig, name: str):
        self.db_config = db_config
        self.name = name.strip().lower()
        self.db_config.filename = f"{self.name}.sqlite"
        self.db_ctx = DbCtx(self.db_config)
        self._collection_documents_table_name = self.name
        self._collection_document_data_table_name = f"{self.name}_data"

        # TODO: consider uuid form TEXT(36) to BLOB(16) for performance and space efficiency
        # each collection is database file with a table named after the collection
        self.db_ctx.c.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self._collection_documents_table_name} ( -- collection of documents table
                uuid TEXT(36) PRIMARY KEY -- document id
            )
            """
        )
        self.db_ctx.c.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self._collection_document_data_table_name} ( -- document data table
                uuid TEXT(36) PRIMARY KEY, -- keyvalue id
                doc_id TEXT(36) NOT NULL, -- document uuid
                parent_uuid TEXT(36), -- parent keyvalue id
                key TEXT NOT NULL,
                type integer NOT NULL, -- type enum to map
                value -- sqlite is dynamic typed. Type is a hint.
            )
            """
        )
        self.db_ctx.conn.commit()

    @staticmethod
    def _get_json_value_type(value) -> JsonValueType:
        """maps a type in the json object to a JsonValueType enum"""
        if isinstance(value, dict):
            return JsonValueType.OBJECT
        elif isinstance(value, list):
            return JsonValueType.ARRAY
        elif isinstance(value, str):
            return JsonValueType.STRING
        elif isinstance(value, int):
            return JsonValueType.INTEGER
        elif isinstance(value, float):
            return JsonValueType.FLOAT
        elif isinstance(value, bool):
            return JsonValueType.BOOLEAN
        elif isinstance(value, datetime):
            raise NotImplementedError("Datetime not yet supported")
        elif value is None:
            return JsonValueType.NULL
        else:
            raise TypeError(f"Unsupported JSON value type: {type(value)}")

    @staticmethod
    def _map_json_value_type_to_db_value(value, value_type: JsonValueType):
        """maps a value in the json object to a value in the database"""
        if value_type == JsonValueType.STRING:
            return str(value)
        elif value_type == JsonValueType.INTEGER:
            return int(value)
        elif value_type == JsonValueType.FLOAT:
            return float(value)
        elif value_type == JsonValueType.BOOLEAN:
            return bool(value)
        elif value_type == JsonValueType.DATETIME:
            raise NotImplementedError("Datetime not yet supported")
            #return value.isoformat()  # ISO8601 string
        elif value_type == JsonValueType.NULL:
            return None
        elif value_type == JsonValueType.OBJECT:  # dict type
            return None
        elif value_type == JsonValueType.ARRAY:  # list type
            return None
        else:
            raise ValueError(f"Unsupported JSON value type: {value_type}")

    def insert_one(self, document: Mapping[str, Any] | str, uuid: Optional[ObjectId] = None) -> ObjectId:
        """Add a document to the collection.
        Returns:
            ObjectId: The uuid of the newly created document.
        """


        doc_id = uuid or ObjectId()

        #print("add gen doc_id: ", doc_id)

        # parse the json, recurse through the json nodes, and insert each node into the table
        def recurse_and_insert(node, parent_uuid):

            if isinstance(node, dict): # node is JSON object
                items = node.items()
            elif isinstance(node, list): # node is a JSON array
                items = enumerate(node)
            else:
                raise ValueError(f"Unsupported JSON value type '{type(node)}'")
            
            for key, value in items:
                child_uuid = ObjectId()
                _type = self._get_json_value_type(value)
                _value = self._map_json_value_type_to_db_value(value, _type)
                #print("key: ", key, "_type: ", _type, "_value: ", _value)
                self.db_ctx.c.execute(
                    f"""
                    INSERT INTO {self._collection_document_data_table_name} (uuid, doc_id, parent_uuid, key, type, value)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        str(child_uuid),
                        str(doc_id),
                        str(parent_uuid),
                        key,
                        _type.value,
                        _value,
                    ),  # Assuming type 1 for dict
                )
                if _value is None:  # not leaf node, so keep recursing
                    recurse_and_insert(value, child_uuid)  # array or object so recurse

                

        try:
            self.db_ctx.c.execute("BEGIN TRANSACTION")
            self.db_ctx.c.execute(
                f"""
              INSERT INTO {self._collection_documents_table_name} (uuid)
              VALUES (?)
              """,
                (str(doc_id),)
            )

            doc_data = json.loads(document) if isinstance(document, str) else document
            recurse_and_insert(doc_data, None)
            # self.db_ctx.c.execute("END TRANSACTION")
            self.db_ctx.conn.commit()
            return doc_id
        except Exception as e:
            self.db_ctx.c.execute("ROLLBACK TRANSACTION")
            self.db_ctx.conn.commit()
            raise e

    def find_one(self, uuid: ObjectId) -> Any:
        """Get a document from the collection.
        Returns:
            Any: The document.
        """
        self.db_ctx.c.execute(
            f"""
            SELECT uuid, doc_id, parent_uuid, key, type, value FROM {self._collection_document_data_table_name}
            WHERE doc_id = ?
            """,
            (str(uuid),),
        )
        all_nodes_data = self.db_ctx.c.fetchall()

        #print(all_nodes_data)
        #print("all nodes len: ", len(all_nodes_data))

        # Initialize the root node
        output_doc = {}

        # Use a stack to process nodes iteratively
        stack: list[tuple[Optional[str], Union[dict, list]]] = [(str(None), output_doc)] # this has to be a str(None) because the database return None str type for NULL rather than NoneType

        while stack:
            parent_uuid, parent_node = stack.pop()
            #print("parent_uuid:", parent_uuid, "Type:", type(parent_uuid))
            for node in all_nodes_data:
                #print("node[2]:", node[2], "Type:", type(node[2]))
                if node[2] == parent_uuid:
                    key = node[3]
                    #print("key name: ", key)
                    value_type = JsonValueType(node[4])
                    if value_type == JsonValueType.OBJECT:
                        child_node = {}
                        if isinstance(parent_node, list): # parent array
                            parent_node.insert(int(key)+1, child_node) # here key is be the original index position in the array.
                        elif isinstance(parent_node, dict): #parent also object
                            parent_node[key] = child_node

                        stack.append((node[0], child_node))
                    elif value_type == JsonValueType.ARRAY:
                        child_node = []
                        #parent_node[key] = child _node
                        if isinstance(parent_node, list): # parent also array
                            parent_node.insert(int(key)+1, child_node) # here key is be the original index position in the array.
                        elif isinstance(parent_node, dict): # parent is object
                            parent_node[key] = child_node

                        # Add the array itself to the stack
                        stack.append((node[0], child_node))
                    else: # leaf node
                        leaf_value = self._map_json_value_type_to_db_value(
                                node[5], value_type
                            )
                        if isinstance(parent_node, list): # parent also array
                            parent_node.insert(int(key)+1, leaf_value) # here key is be the original index position in the array.
                        elif isinstance(parent_node, dict): # parent is object
                            parent_node[key] = leaf_value
        return output_doc

    def get_all(self):
        pass

    def update(self, id, title, content):
        pass

    def delete(self, id):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.db_ctx.conn.close()


class DocDbLite:
    def __init__(self, db_dir: str):
        self.db_dir = db_dir
        self.system_db_ctx = DbCtx(DbConfig(self.db_dir, "system.sqlite"))
        self._create_collections_table()

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
        return Collection(db_cfg, name)
