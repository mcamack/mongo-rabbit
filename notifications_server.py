import asyncio
import json
import os
import uvicorn

from aio_pika import connect_robust, ExchangeType, Message, DeliveryMode, Channel
from aiormq.exceptions import AMQPConnectionError
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Header
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.collection import Collection
from pymongo.database import Database

# Get env vars
NOTIFICATIONS_PORT =        os.getenv('NOTIFICATIONS_PORT', 8000)
NOTIFICATIONS_WORKERS =     os.getenv('NOTIFICATIONS_WORKERS', 1)

MONGODB_USER =              os.getenv('MONGODB_USER')
MONGODB_PASSWORD =          os.getenv('MONGODB_PASSWORD')
MONGODB_HOST =              os.getenv('MONGODB_HOST', 'localhost')  # Default to localhost if not set
MONGODB_PORT =              os.getenv('MONGODB_PORT', 27017)  # Default to 27017 if not set

MONGODB_DATABASE =          os.getenv('MONGODB_DATABASE', 'users')
MONGODB_COLLECTION =        os.getenv('MONGODB_DATABASE', 'subscriptions')

RABBITMQ_USER =             os.getenv('RABBITMQ_USER', 'user')
RABBITMQ_PASSWORD =         os.getenv('RABBITMQ_PASSWORD', 'pass')
RABBITMQ_HOST =             os.getenv('RABBITMQ_HOST', 'localhost')
RABBITMQ_EXCHANGE_NAME =    os.getenv('RABBITMQ_EXCHANGE_NAME', "notifications")  # Default to notifications if not set
RABBITMQ_BROADCAST_KEY =    "global-broadcast"

@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        # Create RabbitMQ connection during startup
        rabbitmq_connection = await connect_robust(f"amqp://{RABBITMQ_USER}:{RABBITMQ_PASSWORD}@{RABBITMQ_HOST}/")
        print("RabbitMQ connection established.")
        
        # Store connection in app.state
        app.state.rabbitmq_connection = rabbitmq_connection

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

        # {
        #     "detail": "localhost:27017: [SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: 
        #     Hostname mismatch, certificate is not valid for 'localhost'. (_ssl.c:1145) 
        #     (configured timeouts: socketTimeoutMS: 20000.0ms, connectTimeoutMS: 20000.0ms), 
        #     Timeout: 30s, Topology Description: <TopologyDescription id: 66f96b313380277dbbec15c1, 
        #     topology_type: Unknown, servers: [<ServerDescription ('localhost', 27017) server_type: Unknown, 
        #     rtt: None, error=AutoReconnect(\"localhost:27017: [SSL: CERTIFICATE_VERIFY_FAILED] 
        #     certificate verify failed: Hostname mismatch, certificate is not valid for 'localhost'. 
        #     (_ssl.c:1145) (configured timeouts: socketTimeoutMS: 20000.0ms, connectTimeoutMS: 20000.0ms)\")>]>"
        # }

        # Store mongo stuff in app.state
        app.state.db: Database = app.state.mongo_client[MONGODB_DATABASE]
        app.state.collection: Collection = app.state.db[MONGODB_COLLECTION]

        # Yield control back to FastAPI with a RabbitMQ channel open for use
        async with rabbitmq_connection.channel() as channel:
            app.state.rabbitmq_channel = channel

            # Create one exchange to use for all messages. Idempotent.
            print(f"Creating exchange if needed: {RABBITMQ_EXCHANGE_NAME}")
            app.state.exchange = await channel.declare_exchange(RABBITMQ_EXCHANGE_NAME, ExchangeType.TOPIC, durable=True, passive=True)
            yield

        # Close RabbitMQ connection during shutdown
        await rabbitmq_connection.close()
        print("RabbitMQ connection closed.")
        app.state.mongo_client.close()
        print("MongoDB connection closed.")
    except AMQPConnectionError as e:
        print("ERROR: RabbitMQ connection failed!")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Create FastAPI app with lifespan
app = FastAPI(lifespan=lifespan)

async def subscription_transaction(userid: str, topic: str, add: str = True):
    mongo_client: AsyncIOMotorClient = app.state.mongo_client
    collection: Collection = app.state.collection
    rabbitmq_channel: Channel = app.state.rabbitmq_channel

    # Start a MongoDB session for transaction
    async with await mongo_client.start_session() as session:
        async with session.start_transaction():

            try:                
                queue_name = f"queue_{userid}"
                routing_key = f"{topic}"

                if add: # Adding a new subscription
                    update_result = await collection.update_one(
                        {"userid": userid},
                        {"$addToSet": {"subscriptions": topic}},  # Add the topic to the subscriptions array if not already there
                        upsert=True, # Add the userid if it wasn't found
                        session=session  # Perform the update within the transaction
                    )
                    print(f"MongoDB update: {update_result.modified_count} document(s) modified.")
                    
                    await create_rabbitmq_binding(rabbitmq_channel, RABBITMQ_EXCHANGE_NAME, queue_name, routing_key)
                    
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

                    await delete_rabbitmq_binding(rabbitmq_channel, RABBITMQ_EXCHANGE_NAME, queue_name, routing_key)

                    if update_result.modified_count > 0:
                        return f"Deleted subscription to topic: {topic} for user: {userid}"
                    elif update_result.modified_count is not None:
                        return "No updates made."
                    
                    raise HTTPException(status_code=500, detail="Error in subscription_transaction")

                # Step 3: Commit the MongoDB transaction if RabbitMQ succeeded
                session.commit_transaction()
                print("Mongo Transaction committed.")

            except Exception as e:
                # Step 4: Rollback the MongoDB transaction if RabbitMQ binding fails
                print(f"Error occurred: {e}. Rolling back MongoDB transaction.")
                session.abort_transaction()
                print("Transaction aborted.")
                raise HTTPException(status_code=500, detail=str(e))

async def create_rabbitmq_binding(channel: str, exchange_name: str, queue_name: str, routing_key: str):
    try:
        queue = await channel.declare_queue(queue_name, passive=False, exclusive=False, durable=True)
        await queue.bind(exchange_name, routing_key)
        print(f"RabbitMQ binding created. Exchange: {exchange_name}, Queue: {queue_name}, Routing Key: {routing_key}")
    except Exception as e:
        print(f"Failed to create RabbitMQ binding: {e}")
        raise e  # Propagate the exception

async def delete_rabbitmq_binding(channel: str, exchange_name: str, queue_name: str, routing_key: str):
    try:
        queue = await channel.declare_queue(queue_name, passive=False, exclusive=False, durable=True)
        await queue.unbind(exchange_name, routing_key)
        print(f"RabbitMQ binding deleted: {exchange_name}, {queue_name}, {routing_key}")
    except Exception as e:
        print(f"Failed to delete RabbitMQ binding: {e}")
        raise e  # Propagate the exception


############################### Subscription Routes ###############################

@app.post("/notifications/subscription/{topic}")
async def add_subscription(topic: str, userid: str = Header(None)):
    try:
        result = await subscription_transaction(userid, topic)
        return {"message": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/notifications/subscription/{topic}")
async def delete_subscription(topic: str, userid: str = Header(None)):
    try:
        result = await subscription_transaction(userid, topic, add=False)
        return {"message": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/notifications/subscription")
async def get_subscriptions(userid: str = Header(None)):
    try:
        collection = app.state.collection

        async for document in collection.find({"userid": userid}, {"subscriptions": 1}):
            return document["subscriptions"]
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

############################### Message Routes ###############################

@app.post("/notifications/message/{topic}")
async def publish_message(topic: str, body: dict):
    try:
        exchange = app.state.exchange
        message = Message(body=json.dumps(body).encode(), delivery_mode=DeliveryMode.PERSISTENT)

        print(f"Publishing message with routing key: {topic}")
        await exchange.publish(routing_key=topic, message=message)
        return {"message": f"Publishing message with routing key: {topic}"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/notifications/message/broadcast/")
async def broadcast_message(body: dict):
    try:
        # The RABBITMQ_BROADCAST_KEY is setup in the RabbitMQ Consumer function for each user queue
        exchange = app.state.exchange
        message = Message(body=json.dumps(body).encode(), delivery_mode=DeliveryMode.PERSISTENT)

        print(f"Broadcasting message.")
        await exchange.publish(routing_key=RABBITMQ_BROADCAST_KEY, message=message)
        return {"message": "Broadcasted message."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

async def main():
    uvicorn.run("__main__:app", host="0.0.0.0", port=NOTIFICATIONS_PORT, reload=True, workers=NOTIFICATIONS_WORKERS)

if __name__ == "__main__":
    asyncio.run(main())
