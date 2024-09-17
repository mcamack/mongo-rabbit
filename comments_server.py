from fastapi import FastAPI, HTTPException, Header
from typing import Dict, Any
from datetime import datetime
from bson.json_util import dumps
import motor.motor_asyncio
import uvicorn
import os
from json import loads
from aio_pika import connect_robust, ExchangeType
from aio_pika.abc import AbstractRobustConnection
import asyncio
from aiormq.exceptions import ProbableAuthenticationError
from contextlib import asynccontextmanager

@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        # Create RabbitMQ connection during startup
        rabbitmq_connection = await connect_robust("amqp://user:pass@localhost/")
        print("RabbitMQ connection established.")
        
        # Store connection in app.state
        app.state.rabbitmq_connection = rabbitmq_connection
        # app.state.rabbitmq_channel = rabbitmq_connection.channel()

        # Yield control back to FastAPI with a RabbitMQ channel open for use
        async with rabbitmq_connection.channel() as channel:
            app.state.rabbitmq_channel = channel
            yield

        # Close RabbitMQ connection during shutdown
        await rabbitmq_connection.close()
        print("RabbitMQ connection closed.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Create FastAPI app with lifespan
app = FastAPI(lifespan=lifespan)

# MongoDB Connection
mongo_user = os.getenv('MONGODB_USER')
mongo_password = os.getenv('MONGODB_PASSWORD')
mongo_host = os.getenv('MONGODB_HOST', 'localhost')  # Default to localhost if not set
mongo_port = os.getenv('MONGODB_PORT', 27017)  # Default to 27017 if not set
mongo_client = motor.motor_asyncio.AsyncIOMotorClient(
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
