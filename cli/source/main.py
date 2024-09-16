import json

from docdblite import DocDbLite


def main():
    db = DocDbLite()

    testCollection = db.add_collection("test")
    doc_id = testCollection.insert_one(document='{"key": "value"}')
    print("doc id: ", doc_id)
    doc = testCollection.find_one(doc_id)

    print("reloaded doc: ", doc)



    test_json_str = """{
  "catalog": {
    "lastUpdated": "2024-09-14T10:30:00Z",
    "version": "2.5.1",
    "categories": [
      {
        "id": "ELEC001",
        "name": "Electronics",
        "subcategories": [
          {
            "id": "SMART001",
            "name": "Smartphones",
            "products": [
              {
                "id": "SP12345",
                "name": "TechPro X1",
                "brand": "TechPro",
                "price": 799.99,
                "currency": "USD",
                "inStock": true,
                "specifications": {
                  "display": "6.5 inch OLED",
                  "processor": "OctoCore 2.4GHz",
                  "ram": "8GB",
                  "storage": "256GB",
                  "camera": {
                    "rear": "Triple 48MP + 12MP + 8MP",
                    "front": "24MP"
                  },
                  "battery": "4500mAh"
                },
                "reviews": [
                  {
                    "userId": "U789012",
                    "rating": 4.5,
                    "comment": "Great phone, amazing camera!",
                    "date": "2024-08-30T14:22:10Z"
                  },
                  {
                    "userId": "U456789",
                    "rating": 5,
                    "comment": "Best smartphone I've ever owned.",
                    "date": "2024-09-05T09:11:32Z"
                  }
                ]
              },
              {
                "id": "SP67890",
                "name": "GalaxyMax Pro",
                "brand": "Samstar",
                "price": 1099.99,
                "currency": "USD",
                "inStock": false,
                "specifications": {
                  "display": "6.8 inch AMOLED",
                  "processor": "DecaCore 3.0GHz",
                  "ram": "12GB",
                  "storage": "512GB",
                  "camera": {
                    "rear": "Quad 108MP + 48MP + 12MP + 8MP",
                    "front": "40MP"
                  },
                  "battery": "5000mAh"
                },
                "reviews": [
                  {
                    "userId": "U123456",
                    "rating": 4.8,
                    "comment": "Incredible performance and camera quality!",
                    "date": "2024-09-10T16:45:22Z"
                  }
                ]
              }
            ]
          },
          {
            "id": "LAPT001",
            "name": "Laptops",
            "products": [
              {
                "id": "LT54321",
                "name": "UltraBook Pro",
                "brand": "TechPro",
                "price": 1499.99,
                "currency": "USD",
                "inStock": true,
                "specifications": {
                  "display": "15.6 inch 4K IPS",
                  "processor": "Intel i9-13900H",
                  "ram": "32GB",
                  "storage": "1TB SSD",
                  "graphics": "NVIDIA RTX 4080",
                  "battery": "8 hours"
                },
                "reviews": [
                  {
                    "userId": "U345678",
                    "rating": 4.7,
                    "comment": "Powerful and sleek, perfect for work and gaming!",
                    "date": "2024-09-12T11:33:45Z"
                  }
                ]
              }
            ]
          }
        ]
      },
      {
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
                    "date": "2024-09-08T19:17:03Z"
                  }
                ]
              }
            ]
          }
        ]
      }
    ]
  }
}"""

    doc_id2 = testCollection.insert_one(document=test_json_str)
    
    doc2 = testCollection.find_one(doc_id2)

    doc2_str = json.dumps(doc2, indent=2)
    #assert doc2 == test_json_str
    #assert doc2 == test_json_str

    print("reloaded doc2: ", doc2_str)




if __name__ == "__main__":
  main()