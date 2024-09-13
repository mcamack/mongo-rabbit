#! /usr/bin/env python3

from pymongo import MongoClient
from datetime import datetime

# Connect to the MongoDB server (localhost:27017 by default)
MONGO_URI = "mongodb://localhost:27017/"
client = MongoClient(MONGO_URI)

# Databases and their collections to be created
databases = {
    "users": ["profile", "products"],
    "comments": ["orders", "customers"],
    "requirements": ["reviews", "transactions"]
}

# Loop through each database and its collections
for db_name, collections in databases.items():
    db = client[db_name]  # Access the database
    for collection_name in collections:
        if collection_name not in db.list_collection_names():
            db.create_collection(collection_name)  # Create the collection
            print(f"Collection '{collection_name}' created in database '{db_name}'")
        else:
            print(f"Collection '{collection_name}' already exists in database '{db_name}'")

# Close the connection when done
client.close()