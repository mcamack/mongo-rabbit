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

# MongoDB Database and Collection
mongo_database = os.getenv('MONGODB_DATABASE', 'users')
mongo_collection = os.getenv('MONGODB_DATABASE', 'subscriptions')

db = mongo_client[mongo_database]
collection = db[mongo_collection]

async def subscription_transaction(userid, topic, add=True):
    rabbitmq_channel = app.state.rabbitmq_channel

    # Start a MongoDB session for transaction
    async with await mongo_client.start_session() as session:
        async with session.start_transaction():

            try:                
                exchange_name = 'testB'
                queue_name = f"queue_{userid}"
                routing_key = f"topic.{topic}"    

                if add: # Adding a new subscription
                    update_result = await collection.update_one(
                        {"userid": userid},
                        {"$addToSet": {"subscriptions": topic}},  # Add the topic to the subscriptions array if not already there
                        upsert=True, # Add the userid if it wasn't found
                        session=session  # Perform the update within the transaction
                    )
                    print(f"MongoDB update: {update_result.modified_count} document(s) modified.")
                    print(update_result)
                    
                    await create_rabbitmq_binding(rabbitmq_channel, exchange_name, queue_name, routing_key)
                    
                    if update_result.modified_count > 0:
                        return f"Added subscription to topic: {topic} for user: {userid}"
                    elif update_result.modified_count is not None:
                        return "No updates made."
                else: # Removing a subscription
                    update_result = await collection.update_one(
                        {"userid": userid},
                        {"$pull": {"subscriptions": topic}},  # Remove the topic from the subscriptions array if there
                        upsert=True, # Add the userid if it wasn't found
                        session=session  # Perform the update within the transaction
                    )
                    print(f"MongoDB update: {update_result.modified_count} document(s) modified.")

                    await delete_rabbitmq_binding(rabbitmq_channel, exchange_name, queue_name, routing_key)

                    if update_result.modified_count > 0:
                        return f"Deleted subscription to topic: {topic} for user: {userid}"
                    elif update_result.modified_count is not None:
                        return "No updates made."

                # Step 3: Commit the MongoDB transaction if RabbitMQ succeeded
                session.commit_transaction()
                print("Mongo Transaction committed.")

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

async def delete_rabbitmq_binding(channel, exchange_name, queue_name, routing_key):
    try:
        queue = await channel.declare_queue(queue_name, passive=False, exclusive=False, durable=True)
        await queue.unbind(exchange_name, routing_key)
        print(f"RabbitMQ binding deleted: {exchange_name}, {queue_name}, {routing_key}")
    except Exception as e:
        print(f"Failed to delete RabbitMQ binding: {e}")
        raise e  # Propagate the exception


############################### Routes ###############################

@app.post("/subscription/{topic}")
async def add_subscription(topic: str, userid: str = Header(None)):
    try:
        result = await subscription_transaction(userid, topic)
        return {"message": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/subscription/{topic}")
async def delete_subscription(topic: str, userid: str = Header(None)):
    try:
        result = await subscription_transaction(userid, topic, add=False)
        return {"message": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/subscription")
async def get_subscriptions(userid: str = Header(None)):
    try:
        async for document in collection.find({"userid": userid}, {"subscriptions": 1}):
            return document["subscriptions"]
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
async def main():
    uvicorn.run("__main__:app", host="0.0.0.0", port=8000, reload=True, workers=2)

if __name__ == "__main__":
    asyncio.run(main())
