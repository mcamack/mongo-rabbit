import sys
import threading
from pymongo import MongoClient
from pymongo.errors import PyMongoError
import asyncio
import os
import aio_pika
from aio_pika.pool import Pool
from aio_pika.abc import AbstractRobustConnection

# Connect to the MongoDB server (localhost:27017 by default)
MONGODB_USER =              os.getenv('MONGODB_USER')
MONGODB_PASSWORD =          os.getenv('MONGODB_PASSWORD')
MONGODB_HOST =              os.getenv('MONGODB_HOST', 'localhost')  # Default to localhost if not set
MONGODB_PORT =              os.getenv('MONGODB_PORT', 27017)  # Default to 27017 if not set

MONGODB_DATABASE =          os.getenv('MONGODB_DATABASE', 'users')
MONGODB_COLLECTION =        os.getenv('MONGODB_DATABASE', 'subscriptions')

connection_string = f"mongodb://{MONGODB_USER}:{MONGODB_PASSWORD}@{MONGODB_HOST}:{MONGODB_PORT}"
client = MongoClient(connection_string,
    tls=True,
    tlsCAFile='/tmp/mongotest2/ca.crt',
    tlsCertificateKeyFile='client.pem',
    # tlsAllowInvalidCertificates=False,  # Enforce strict certificate validation   
    tlsAllowInvalidHostnames=True 
    )
db = client[MONGODB_DATABASE]

async def rabbitmq_create_pool():
    print("Creating RabbitMQ pool...")
    async def get_connection() -> AbstractRobustConnection:
        return await aio_pika.connect_robust("amqp://user:pass@localhost/")

    connection_pool: Pool = Pool(get_connection, max_size=2)

    async def get_channel() -> aio_pika.Channel:
        async with connection_pool.acquire() as connection:
            return await connection.channel()

    channel_pool: Pool = Pool(get_channel, max_size=10)

    async with channel_pool.acquire() as channel:  # type: aio_pika.Channel
        await channel.set_qos(10)

    async def create_rabbitmq_binding(channel, exchange_name, queue_name, routing_key):
        try:
            await channel.declare_exchange(exchange_name, aio_pika.ExchangeType.TOPIC, durable=True)
            queue = await channel.declare_queue(queue_name, passive=False, exclusive=False, durable=True)
            await queue.bind(exchange_name, routing_key)
            print(f"RabbitMQ binding created: {exchange_name}, {queue_name}, {routing_key}")
        except Exception as e:
            print(f"Failed to create RabbitMQ binding: {e}")
            raise e  # Propagate the exception
    
    async with channel_pool.acquire() as channel:    
        await create_rabbitmq_binding(channel, "testExchangeA", "testQueueA","testRoutingKeyA")
        
async def monitor_changes():
    """Open a change stream on the collection"""

    while True: # run indefinitely
        try:
            with db.watch() as change_stream:
                print(f"Monitoring changes for ...")
                for change in change_stream:
                    print("Change detected:", change)
                    await rabbitmq_create_pool()
                    # create_rabbitmq_binding(channel, exchange_name, queue_name, routing_key)

            asyncio.sleep(1)
        except KeyboardInterrupt:
            print(f"Closing Watcher")
            sys.exit(0)

# {'_id': {'_data': '8266D3EF23000000012B042C0100296E5A10045C1BE20648104131A527B9EAD80E9F75463C6F7065726174696F6E54797065003C696E736572740046646F63756D656E744B65790046645F6964006466D3EF237469C95EDC2C0B49000004'}, 
#  'operationType': 'insert', 
#  'clusterTime': Timestamp(1725165347, 1), 
#  'wallTime': datetime.datetime(2024, 9, 1, 4, 35, 47, 908000), 
#  'fullDocument': {'_id': ObjectId('66d3ef237469c95edc2c0b49'), 'doc': 'doc1', 'body': 'api works for new docs!', 'timestamp': datetime.datetime(2024, 8, 31, 21, 35, 47, 902000)}, 'ns': {'db': 'comments', 'coll': 'doc1'}, 'documentKey': {'_id': ObjectId('66d3ef237469c95edc2c0b49')}}

async def main():
    await asyncio.create_task(monitor_changes())

asyncio.run(main())
