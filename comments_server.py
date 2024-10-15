import asyncio
import os
import uvicorn

from bson.objectid import ObjectId
from contextlib import asynccontextmanager
from datetime import datetime
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.database import Database
from typing import Dict, Any

def bson_to_json_serializable(document):
    if isinstance(document, list):
        return [bson_to_json_serializable(item) for item in document]
    elif isinstance(document, dict):
        return {key: bson_to_json_serializable(value) for key, value in document.items()}
    elif isinstance(document, ObjectId):
        return str(document)
    else:
        return document
    

# Get env vars
COMMENTS_PORT =             os.getenv('COMMENTS_PORT', 8001)
COMMENTS_WORKERS =          os.getenv('COMMENTS_WORKERS', 1)

MONGODB_USER =              os.getenv('MONGODB_USER')
MONGODB_PASSWORD =          os.getenv('MONGODB_PASSWORD')
MONGODB_HOST =              os.getenv('MONGODB_HOST', 'localhost')  # Default to localhost if not set
MONGODB_PORT =              os.getenv('MONGODB_PORT', 27017)  # Default to 27017 if not set

MONGODB_DATABASE =          os.getenv('MONGODB_DATABASE', 'comments')

@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        # MongoDB Connection
        app.state.mongo_client = AsyncIOMotorClient(
            # f"mongodb://{mongo_user}:{mongo_password}@{mongo_host}:{mongo_port}/?authSource=admin&ssl=true",
            f"mongodb://{MONGODB_USER}:{MONGODB_PASSWORD}@{MONGODB_HOST}:{MONGODB_PORT}",
            # read_preference='secondaryPreferred',
            # write_concern={'w': 'majority'},
            tls=True,
            # tlsCAFile='./generated-cert.pem',  # Path to the CA certificate
            # tlsCAFile='/tmp/mongotest/mongodb.pem',  # Path to the CA certificate
            tlsCAFile='/tmp/mongotest2/ca.crt',  # Path to the CA certificate
            # tlsCertificateKeyFile='./generated-key2.pem',  # Path to the client certificate (optional)
            # tlsCertificateKeyFile='/tmp/mongotest/mongodb-client.pem',  # Path to the client certificate (optional)
            tlsCertificateKeyFile='client.pem',  # Path to the client certificate (optional)
            tlsAllowInvalidCertificates=False,  # Enforce strict certificate validation   
            tlsAllowInvalidHostnames=True,         
            maxPoolSize=10,  # Max connections in the pool
            minPoolSize=5   # Min connections in the pool
        )        

        # Store mongo stuff in app.state
        app.state.db: Database = app.state.mongo_client[MONGODB_DATABASE]

        # Yield control back to FastAPI
        yield

        # Close MongoDB connection during shutdown
        app.state.mongo_client.close()
        print("MongoDB connection closed.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Create FastAPI app with lifespan
app = FastAPI(lifespan=lifespan)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # List the allowed origins, can be "*" for all
    allow_credentials=True,
    allow_methods=["*"],  # Allow all HTTP methods
    allow_headers=["*"],  # Allow all headers
)

@app.post("/comment/{topic}")
async def add_comment(topic: str, payload: Dict[str, Any]):
    try:
        collection = app.state.db[topic]

        # Insert the comment into the MongoDB collection
        payload["timestamp"] = datetime.now() 
        result = await collection.insert_one(payload)
        return {"message": "Comment added successfully", "id": str(result.inserted_id)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/comment/{topic}/{docid}")
async def delete_subscription(topic: str, docid: str):
    try:
        collection = app.state.db[topic]

        # Convert the string document_id to ObjectId
        object_id = ObjectId(docid)
        
        # Delete the document based on the _id field
        result = await collection.delete_one({"_id": object_id})
        
        if result.deleted_count > 0:
            return {"message": f"Document with _id {docid} was deleted."}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    raise HTTPException(status_code=404, detail="Document ID not found.")
    
@app.get("/comment/{topic}")
async def get_comment(topic: str):
    try:
        collection = app.state.db[topic]

       # Retrieve all documents from the 'comments' collection
        comments_cursor =  collection.find()
        comments_list = [doc async for doc in comments_cursor]

        # Convert the list of comments to JSON using bson.json_util.dumps
        return bson_to_json_serializable(comments_list)

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@app.get("/comment/{topic}/{docid}")
async def get_comment(topic: str, docid: str):
    try:
        collection = app.state.db[topic]

        # Convert the string document_id to ObjectId if necessary
        object_id = ObjectId(docid)

        # Retrieve all documents from the 'comments' collection
        document  =  await collection.find_one({"_id": object_id})

        if document:
            return bson_to_json_serializable(document)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
    raise HTTPException(status_code=404, detail="Document ID not found.")

async def main():
    uvicorn.run("__main__:app", host="0.0.0.0", port=COMMENTS_PORT, reload=True, workers=COMMENTS_WORKERS)

if __name__ == "__main__":
    asyncio.run(main())
