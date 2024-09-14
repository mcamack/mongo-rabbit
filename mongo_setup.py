#! /usr/bin/env python3

from pymongo import MongoClient
from datetime import datetime
import os

# Connect to the MongoDB server (localhost:27017 by default)
mongo_user = os.getenv('MONGODB_USER')
mongo_password = os.getenv('MONGODB_PASSWORD')
mongo_host = os.getenv('MONGODB_HOST', 'localhost')  # Default to localhost if not set
mongo_port = os.getenv('MONGODB_PORT', 27017)  # Default to 27017 if not set

connection_string = f"mongodb://{mongo_user}:{mongo_password}@{mongo_host}:{mongo_port}"
client = MongoClient(connection_string)

# Databases and their collections to be created
databases = {
    "users": ["test"],
    "comments": ["test"],
    "requirements": ["test"]
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