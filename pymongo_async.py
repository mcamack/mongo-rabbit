from fastapi import FastAPI, HTTPException, Request
from typing import Dict, Any
from datetime import datetime
from bson.json_util import dumps
import motor.motor_asyncio
import uvicorn
import os

app = FastAPI()

# MongoDB connection
mongo_user = os.getenv('MONGODB_USER')
mongo_password = os.getenv('MONGODB_PASSWORD')
mongo_host = os.getenv('MONGODB_HOST', 'localhost')  # Default to localhost if not set
mongo_port = os.getenv('MONGODB_PORT', 27017)  # Default to 27017 if not set
client = motor.motor_asyncio.AsyncIOMotorClient(
    f"mongodb://{mongo_user}:{mongo_password}@{mongo_host}:{mongo_port}",
    # read_preference='secondaryPreferred',
    # write_concern={'w': 'majority'},
    # ssl=True,
    # ssl_certfile='/path/to/cert.pem',
    # ssl_keyfile='/path/to/key.pem',
    # ssl_ca_certs='/path/to/ca.pem',
    maxPoolSize=10,  # Max connections in the pool
    minPoolSize=5   # Min connections in the pool
)

db = client['comments']
users_db = client['users']

@app.post("/add_comment/{topic}")
async def add_comment(topic: str, payload: Dict[str, Any]):
    collection = db[topic]
    try:
        # Insert the comment into the MongoDB collection
        payload["timestamp"] = datetime.now() 
        result = await collection.insert_one(payload)
        return {"message": "Comment added successfully", "id": str(result.inserted_id)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/add_subscription/{topic}")
async def add_comment(topic: str, payload: Dict[str, Any]):
    user_id = "matt" #TODO pull from headers
    # Check if user exists in the database
    try:
        result = await users_db["subscriptions"].update_one(
            {"_id": user_id},
            {"$addToSet": {"subscriptions": topic}},  # Add the topic if not already present
            upsert=True  # Create the document if the user doesn't exist
        )

        if result.matched_count > 0:
            return {"message": f"Subscribed to topic: {topic} (existing user)"}
        elif result.upserted_id is not None:
            return {"message": f"Subscribed to topic: {topic} (new user created)"}

        raise HTTPException(status_code=500, detail="Failed to add subscription")


        # collection = users_db["subscriptions"]

        # # Insert the comment into the MongoDB collection
        # payload["timestamp"] = datetime.now() 
        # result = await collection.insert_one(payload)
        # return {"message": "Comment added successfully", "id": str(result.inserted_id)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/get_comment/{topic}")
async def get_comment(topic: str):
    # collection = db["doc1"]
    collection = db[topic]

    try:
        # Retrieve all documents from the 'comments' collection
        comments_cursor =  collection.find()
        comments_list = [doc async for doc in comments_cursor]

        # Convert the list of comments to JSON using bson.json_util.dumps
        comments_json = dumps(comments_list)

        return comments_json
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
if __name__ == "__main__":
    uvicorn.run("__main__:app", host="0.0.0.0", port=8000, reload=True, workers=2)

# curl -X GET http://127.0.0.1:8000/get_comment/doc1 -H "Content-Type: application/json"
# curl -X POST http://127.0.0.1:8000/add_comment/doc1 -H "Content-Type: application/json" -d '{"body":"api works for new docs!"}'
# curl -X POST http://127.0.0.1:8000/add_subscription/topicA -H "Content-Type: application/json" -d '{"body":"api works for new docs!"}'
