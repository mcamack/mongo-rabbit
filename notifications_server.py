from fastapi import FastAPI, HTTPException, Request, Depends
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

db = mongo_client['comments']
users_db = mongo_client['users']

async def subscription_transaction(user_id, topic):
    # rabbitmq_connection = app.state.rabbitmq_connection  # Access the connection directly
    rabbitmq_channel = app.state.rabbitmq_channel  # Access the channel directly

    # Start a MongoDB session for transaction
    async with await mongo_client.start_session() as session:
        async with session.start_transaction():

            try:
                # Step 1: Update MongoDB to save the user's subscription
                update_result = await db["subscriptions"].update_one(
                    {"user_id": user_id},
                    {"$addToSet": {"subscriptions": topic}},  # Add the topic to the subscriptions array if not already there
                    upsert=True,
                    session=session  # Perform the update within the transaction
                )
                print(f"MongoDB update: {update_result.modified_count} document(s) modified.")
                
                exchange_name = 'testB'
                queue_name = f"queue_{user_id}"
                routing_key = f"topic.{topic}"    

                # Step 2: Create RabbitMQ binding
                await create_rabbitmq_binding(rabbitmq_channel, exchange_name, queue_name, routing_key)

                # Step 3: Commit the MongoDB transaction if RabbitMQ succeeded
                session.commit_transaction()
                print("Mongo Transaction committed.")

                if update_result.modified_count > 0:
                    return f"Added subscription to topic: {topic} for user: {user_id}"
                elif update_result.modified_count is not None:
                    return "No updates made."

            except Exception as e:
                # Step 4: Rollback the MongoDB transaction if RabbitMQ binding fails
                print(f"Error occurred: {e}. Rolling back MongoDB transaction.")
                session.abort_transaction()
                print("Transaction aborted.")
                raise HTTPException(status_code=500, detail=str(e))

async def create_rabbitmq_binding(channel, exchange_name, queue_name, routing_key):
    try:
        await channel.declare_exchange(exchange_name, ExchangeType.TOPIC, durable=True)
        queue = await channel.declare_queue(queue_name, passive=False, exclusive=False, durable=True)
        await queue.bind(exchange_name, routing_key)
        print(f"RabbitMQ binding created: {exchange_name}, {queue_name}, {routing_key}")
    except Exception as e:
        print(f"Failed to create RabbitMQ binding: {e}")
        raise e  # Propagate the exception
        
@app.post("/subscription/{topic}")
async def add_subscription(topic: str):
    user_id = "matt" #TODO pull from headers

    try:
        result = await subscription_transaction(user_id, topic)
        return {"message": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

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

@app.delete("/subscription/{topic}")
async def delete_subscription(topic: str):
    user_id = "matt" #TODO pull from headers
    # Check if user exists in the database
    try:
        result = await users_db["subscriptions"].update_one(
            {"_id": user_id},
            {"$pull": {"subscriptions": topic}}  # Add the topic if not already present
        )

        if result.modified_count > 0:
            return {"message": f"Removed subscription to topic: {topic}"}
        elif result.modified_count is not None:
            return {"message": f"{topic} topic not found"}

        raise HTTPException(status_code=500, detail="Failed to add subscription")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/comment/{topic}")
async def get_comment(topic: str):
    # collection = db["doc1"]
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

@app.get("/subscription")
async def get_subscriptions():
    user_id = "matt" #TODO pull from headers
    collection = users_db["subscriptions"]

    try:
        # Retrieve all documents from the 'comments' collection
        comments_cursor =  collection.find({"_id": user_id})
        comments_list = [doc async for doc in comments_cursor]

        # Convert the list of comments to JSON using bson.json_util.dumps
        # comments_json = dumps(comments_list)
        comments_json = comments_list

        return comments_json
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
async def main():
    # await asyncio.create_task(rabbitmq_connect())
    uvicorn.run("__main__:app", host="0.0.0.0", port=8000, reload=True, workers=2)

if __name__ == "__main__":
    asyncio.run(main())

# curl -X GET http://127.0.0.1:8000/comment/doc1 -H "Content-Type: application/json"
# curl -X POST http://127.0.0.1:8000/comment/doc1 -H "Content-Type: application/json" -d '{"body":"api works for new docs!"}'

# curl -X GET http://127.0.0.1:8000/subscription -H "Content-Type: application/json" | jq
# curl -X POST http://127.0.0.1:8000/subscription/topicA -H "Content-Type: application/json" -d '{"body":"api works for new docs!"}'
# curl -X DELETE http://127.0.0.1:8000/subscription/topicA -H "Content-Type: application/json" | jq