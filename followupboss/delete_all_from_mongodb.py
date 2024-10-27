from pymongo import MongoClient
client = MongoClient()
db = client.Christina
collection = db.followupboss_people_relationships

def delete_all():
    result = collection.delete_many({})
    print(f"Deleted {result.deleted_count} documents from {collection.name}")

delete_all()