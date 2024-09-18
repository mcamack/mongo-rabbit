from fastapi import FastAPI, HTTPException
from typing import Dict, Any
from datetime import datetime
from bson.json_util import dumps
import motor.motor_asyncio
import uvicorn
import os
from json import loads
from aio_pika import connect_robust
import asyncio
from contextlib import asynccontextmanager

# Create FastAPI app with lifespan
app = FastAPI()

# MongoDB Connection
mongo_user = os.getenv('MONGODB_USER')
mongo_password = os.getenv('MONGODB_PASSWORD')
mongo_host = os.getenv('MONGODB_HOST', 'localhost')  # Default to localhost if not set
mongo_port = os.getenv('MONGODB_PORT', 27017)  # Default to 27017 if not set
mongo_client = motor.motor_asyncio.AsyncIOMotorClient(
    f"mongodb://{mongo_user}:{mongo_password}@{mongo_host}:{mongo_port}",
    maxPoolSize=10,  # Max connections in the pool
    minPoolSize=5   # Min connections in the pool
)

# db = mongo_client['comments']
db = mongo_client['comments']
# collection = db["test"]

@app.post("/comment/{topic}")
async def add_comment(topic: str, payload: Dict[str, Any]):
    collection = db[topic]
    try:
        # Insert the comment into the MongoDB collection
        payload["timestamp"] = datetime.now() 
        result = await collection.insert_one(payload)
        return {"message": "Comment added successfully", "id": str(result.inserted_id)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/comment/{topic}")
async def get_comment(topic: str):
    collection = db[topic]

    try:
        # Retrieve all documents from the 'comments' collection
        comments_cursor =  collection.find()
        comments_list = [doc async for doc in comments_cursor]

        # Convert the list of comments to JSON using bson.json_util.dumps
        comments_json = loads(dumps(comments_list))

        return comments_json
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

async def main():
    uvicorn.run("__main__:app", host="0.0.0.0", port=8001, reload=True, workers=2)

if __name__ == "__main__":
    asyncio.run(main())
