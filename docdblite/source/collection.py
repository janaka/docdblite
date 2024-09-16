import json
from datetime import datetime
from typing import Any, Mapping, Optional, Union

from source.db_config import DbConfig
from source.db_ctx import DbCtx
from source.db_value_type import DbValueType
from source.object_id import ObjectId


class Collection:
    def __init__(self, db_config: DbConfig, name: str):
        self.db_config = db_config
        self.name = name.strip().lower()
        self.db_ctx = DbCtx(self.db_config, name)
        self._collection_documents_table_name = self.name
        self._collection_document_data_table_name = f"{self.name}_data"

        # TODO: consider changing uuid from TEXT(36) to BLOB(16) for performance and space efficiency
        # each collection is database file with a table named after the collection
        with self.db_ctx as db:
            db.conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self._collection_documents_table_name} ( -- collection of documents table
                    uuid TEXT(36) PRIMARY KEY -- document id
                )
                """
            )
            db.conn.execute(
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
            db.conn.commit()

    @staticmethod
    def _get_json_value_type(value) -> DbValueType:
        """maps a type in the json object to a JsonValueType enum"""
        if isinstance(value, dict):
            return DbValueType.OBJECT
        elif isinstance(value, list):
            return DbValueType.ARRAY
        elif isinstance(value, str):
            return DbValueType.STRING
        elif isinstance(value, int):
            return DbValueType.INTEGER
        elif isinstance(value, float):
            return DbValueType.FLOAT
        elif isinstance(value, bool):
            return DbValueType.BOOLEAN
        elif isinstance(value, datetime):
            raise NotImplementedError("Datetime not yet supported")
        elif value is None:
            return DbValueType.NULL
        else:
            raise TypeError(f"Unsupported JSON value type: {type(value)}")

    @staticmethod
    def _map_json_value_type_to_db_value(value, value_type: DbValueType):
        """maps a value in the json object to a value in the database"""
        if value_type == DbValueType.STRING:
            return str(value)
        elif value_type == DbValueType.INTEGER:
            return int(value)
        elif value_type == DbValueType.FLOAT:
            return float(value)
        elif value_type == DbValueType.BOOLEAN:
            return bool(value)
        elif value_type == DbValueType.DATETIME:
            raise NotImplementedError("Datetime not yet supported")
            # return value.isoformat()  # ISO8601 string
        elif value_type == DbValueType.NULL:
            return None
        elif value_type == DbValueType.OBJECT:  # dict type
            return None
        elif value_type == DbValueType.ARRAY:  # list type
            return None
        else:
            raise ValueError(f"Unsupported JSON value type: {value_type}")

    @staticmethod
    def _filter_dict_to_sql_where(filter: Mapping[str, Any]) -> str:
        """Convert a dictionary filter to SQL WHERE criteria."""
        where_clauses = []
        for key, value in filter.items():
            if key == "_id":
                key = "doc_id"

            if isinstance(value, str):
                where_clauses.append(f"key='{key}' AND value='{value}'")
            elif isinstance(value, ObjectId):
                where_clauses.append(f"key='{key}' AND value='{str(value)}'")
            elif isinstance(value, (int, float)):
                where_clauses.append(f"key='{key}' AND value={value}")
            elif isinstance(value, list):
                value_list = ", ".join(
                    f"'{v}'" if isinstance(v, str) else str(v) for v in value
                )
                where_clauses.append(f"{key} IN ({value_list})")
            else:
                raise ValueError(
                    f"Unsupported filter value type: {type(value)} for key: {key}"
                )
        return " AND ".join(where_clauses)

    def insert_one(
        self, document: Mapping[str, Any] | str, uuid: Optional[ObjectId] = None
    ) -> ObjectId:
        """Add a document to the collection.
        Returns:
            ObjectId: The uuid of the newly created document.
        """

        doc_id = uuid or ObjectId()

        # print("add gen doc_id: ", doc_id)

        # parse the json, recurse through the json nodes, and insert each node into the table
        def recurse_and_insert(node, parent_uuid, db: DbCtx):
            if isinstance(node, dict):  # node is JSON object
                items = node.items()
            elif isinstance(node, list):  # node is a JSON array
                items = enumerate(node)
            else:
                raise ValueError(f"Unsupported JSON value type '{type(node)}'")

            for key, value in items:
                child_uuid = ObjectId()
                _type = self._get_json_value_type(value)
                _value = self._map_json_value_type_to_db_value(value, _type)
                # print("key: ", key, "_type: ", _type, "_value: ", _value)

                db.conn.execute(
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
                    recurse_and_insert(
                        value, child_uuid, db
                    )  # array or object so recurse

        with self.db_ctx as db:
            try:
                db.conn.execute("BEGIN TRANSACTION")
                db.conn.execute(
                    f"""
                INSERT INTO {self._collection_documents_table_name} (uuid)
                VALUES (?)
                """,
                    (str(doc_id),),
                )

                doc_data = (
                    json.loads(document) if isinstance(document, str) else document
                )

                recurse_and_insert(doc_data, None, db)

                db.conn.commit()
                return doc_id
            except Exception as e:
                db.conn.rollback()
                raise e

    def find_one(self, uuid: ObjectId) -> Any:
        """Get a document from the collection.
        Returns:
            Any: The document.
        """
        with self.db_ctx as db:
            result = db.conn.execute(
                f"""
                SELECT uuid, doc_id, parent_uuid, key, type, value FROM {self._collection_document_data_table_name}
                WHERE doc_id = ?
                """,
                (str(uuid),),
            )
            all_nodes_data = result.fetchall()

        # print(all_nodes_data)
        # print("all nodes len: ", len(all_nodes_data))

        # Initialize the root node
        output_doc = {}

        # Use a stack to process nodes iteratively
        stack: list[tuple[Optional[str], Union[dict, list]]] = [
            (str(None), output_doc)
        ]  # this has to be a str(None) because the database return None str type for NULL rather than NoneType

        while stack:
            parent_uuid, parent_node = stack.pop()
            # print("parent_uuid:", parent_uuid, "Type:", type(parent_uuid))
            for node in all_nodes_data:
                # print("node[2]:", node[2], "Type:", type(node[2]))
                if node[2] == parent_uuid:
                    key = node[3]
                    # print("key name: ", key)
                    value_type = DbValueType(node[4])
                    if value_type == DbValueType.OBJECT:
                        child_node = {}
                        if isinstance(parent_node, list):  # parent array
                            parent_node.insert(
                                int(key) + 1, child_node
                            )  # here key is be the original index position in the array.
                        elif isinstance(parent_node, dict):  # parent also object
                            parent_node[key] = child_node

                        stack.append((node[0], child_node))
                    elif value_type == DbValueType.ARRAY:
                        child_node = []
                        # parent_node[key] = child _node
                        if isinstance(parent_node, list):  # parent also array
                            parent_node.insert(
                                int(key) + 1, child_node
                            )  # here key is be the original index position in the array.
                        elif isinstance(parent_node, dict):  # parent is object
                            parent_node[key] = child_node

                        # Add the array itself to the stack
                        stack.append((node[0], child_node))
                    else:  # leaf node
                        leaf_value = self._map_json_value_type_to_db_value(
                            node[5], value_type
                        )
                        if isinstance(parent_node, list):  # parent also array
                            parent_node.insert(
                                int(key) + 1, leaf_value
                            )  # here key is be the original index position in the array.
                        elif isinstance(parent_node, dict):  # parent is object
                            parent_node[key] = leaf_value
        return output_doc

    def count_documents(self, filter: Mapping[str, Any]) -> int:
        """Count documents in the collection that match the filter."""

        with self.db_ctx as db:
            result = db.conn.execute(
                f"SELECT DISTINCT COUNT(doc_id) FROM {self._collection_document_data_table_name} WHERE {self._filter_dict_to_sql_where(filter)}",
            )
            count = result.fetchone()[0]
            # print("count: ", self._filter_dict_to_sql_where(filter))
        return int(count)

    def update(self, id, title, content):
        raise NotImplementedError("Update not yet supported")

    def delete_one(self, filter: Mapping[str, Any]) -> None:
        """Delete a document from the collection."""
        with self.db_ctx as db:
            result = db.conn.execute(
                f"SELECT doc_id FROM {self._collection_document_data_table_name} WHERE {self._filter_dict_to_sql_where(filter)}"
            )
            doc_id = result.fetchone()[0]

            db.conn.execute(
                f"DELETE FROM {self._collection_document_data_table_name} WHERE doc_id = ?",
                (doc_id,),
            )
            db.conn.execute(
                f"DELETE FROM {self._collection_documents_table_name} WHERE uuid = ?",
                (doc_id,),
            )
            db.conn.commit()
