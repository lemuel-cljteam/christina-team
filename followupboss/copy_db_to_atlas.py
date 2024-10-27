import pymongo
from pymongo import MongoClient

# Local MongoDB connection
local_client = MongoClient('mongodb://localhost:27017/')
local_db = local_client['Christina']  # Replace with your local DB name

# MongoDB Atlas connection
atlas_client = MongoClient('mongodb+srv://christina:akodcXC3gIB2qhYf@clusterchristina.57107.mongodb.net/')  # Replace with your Atlas connection string
atlas_db = atlas_client['Christina']  # Replace with the target Atlas DB name

# Function to copy collections from local to Atlas
def copy_database(local_db, atlas_db):
    for collection_name in local_db.list_collection_names():
        local_collection = local_db[collection_name]
        atlas_collection = atlas_db[collection_name]

        # Insert documents from local collection to Atlas collection
        documents = local_collection.find()
        if documents:
            atlas_collection.insert_many(documents)
            print(f'Copied {collection_name} to Atlas')

# Start copying process
copy_database(local_db, atlas_db)

print("Database copy complete!")
