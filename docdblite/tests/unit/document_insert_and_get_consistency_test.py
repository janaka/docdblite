"""Check that the json of the doc retrieved from the database matches the input json that was inserted into the database.

This test verified that data is serialised into the documents table and then deserialised back out of the table correctly.
"""
import json

import pytest

from docdblite import DocDbLite

test_json_str = """{
  "id": "HOME001",
  "name": "Home Appliances",
  "subcategories": [
    {
      "id": "KITC001",
      "name": "Kitchen Appliances",
      "products": [
        {
          "id": "KA98765",
          "name": "SmartChef Oven",
          "brand": "HomeTech",
          "price": 599.99,
          "currency": "USD",
          "inStock": true,
          "specifications": {
            "capacity": "30L",
            "functions": ["Bake", "Roast", "Grill", "Air Fry"],
            "connectivity": "Wi-Fi",
            "powerConsumption": "1800W"
          },
          "reviews": [
            {
              "userId": "U901234",
              "rating": 4.6,
              "comment": "Love the smart features and versatility!",
              "date": "2024-09-08T19:17:03Z",
              "something": [
              ["one", "two", "three"],
              ["four", "five", "six"],
              ["seven", "eight", "nine"]
              ]
            }
          ]
        }
      ]
    }
  ]
}"""

@pytest.mark.unit
def test_insert_json_str_and_get_consistency_of_doc() -> None:
    """Insert one document json string and find one by id."""
    db = DocDbLite("../.docdblitedata-tests/db")
    testCollection = db.add_collection("testCollection")

    doc_id = testCollection.insert_one(document=test_json_str)
    doc = testCollection.find_one(doc_id)

    expected_doc = json.loads(test_json_str)

    assert len(expected_doc.items()) == len(doc.items())
    assert expected_doc == doc


@pytest.mark.unit
def test_insert_json_typed_and_get_consistency_of_doc() -> None:
    """Insert one document json string and find one by id."""
    db = DocDbLite("../.docdblitedata-tests/db")
    testCollection = db.add_collection("testCollection")

    doc1 = {"testKey1": "testValue1", "testKey2": {"testKey3": 100}}

    doc_id = testCollection.insert_one(document=doc1)
    doc_result = testCollection.find_one(doc_id)

    assert len(doc1.items()) == len(doc_result.items())
    assert doc1 == doc_result


def test_count_documents() -> None:
    """Insert one document json string and find one by id."""
    db = DocDbLite("../.docdblitedata-tests/db")
    testCollection = db.add_collection("testCollection")

    doc1 = {"testKey1": "testValue1", "testKey2": {"testKey3": 100}}
    doc2 = {"testKey1": "testValue2", "testKey2": {"testKey4": 100}}
    doc3 = {"testKey1": "testValue3", "testKey2": {"testKey5": 100}}

    testCollection.insert_one(document=doc1)
    testCollection.insert_one(document=doc2)
    testCollection.insert_one(document=doc3)

    assert testCollection.count_documents({"testKey1": "testValue1"}) == 1
