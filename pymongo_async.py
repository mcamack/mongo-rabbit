from fastapi import FastAPI, HTTPException, Request
from typing import Dict, Any
from datetime import datetime
from bson.json_util import dumps
import motor.motor_asyncio
import uvicorn

app = FastAPI()

# MongoDB connection
client = motor.motor_asyncio.AsyncIOMotorClient(
    'mongodb://localhost:27017',
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
    uvicorn.run(app, host="0.0.0.0", port=8000)

# curl -X GET http://127.0.0.1:8000/get_comment/doc1 -H "Content-Type: application/json"
# curl -X POST http://127.0.0.1:8000/add_comment/doc1 -H "Content-Type: application/json" -d '{"body":"api works for new docs!"}'
