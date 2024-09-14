"""Check that the json of the doc retrieved from the database matches the input json that was inserted into the database.

This test verified that data is serialised into the documents table and then deserialised back out of the table correctly.
"""
import json

import pytest

from docdblite.source.main import DocDbLite

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
def test_insert_and_get_consistency_of_doc() -> None:
    

    db = DocDbLite("../.docdblitedata-tests/db")
    testCollection = db.add_collection("testCollection")

    doc_id = testCollection.insert_one(document=test_json_str)
    doc = testCollection.find_one(doc_id)

    expected_doc = json.loads(test_json_str)
    
    assert len(expected_doc.items()) == len(doc.items())
    assert expected_doc == doc
