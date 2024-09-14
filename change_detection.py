import sys
import threading
from pymongo import MongoClient
from pymongo.errors import PyMongoError
import asyncio
import os

# Connect to the MongoDB server (localhost:27017 by default)
mongo_user = os.getenv('MONGODB_USER')
mongo_password = os.getenv('MONGODB_PASSWORD')
mongo_host = os.getenv('MONGODB_HOST', 'localhost')  # Default to localhost if not set
mongo_port = os.getenv('MONGODB_PORT', 27017)  # Default to 27017 if not set

connection_string = f"mongodb://{mongo_user}:{mongo_password}@{mongo_host}:{mongo_port}"
client = MongoClient(connection_string)

db = client['comments']
watch_collection = db["doc1"]

def monitor_changes(num: int):
    # Open a change stream on the collection
    print(f"here: {num}")
    while True:
        try:
            with db.watch() as change_stream:
                print(f"Monitoring changes for {num}...")
                for change in change_stream:
                    # Handle the change
                    # print("Change detected:", change)
                    print("Change detected:", change["fullDocument"])
                    # You can add custom logic here to process the change,
                    # such as sending a notification, updating a cache, etc.
            asyncio.sleep(1)
        except KeyboardInterrupt:
            print(f"Closing Watcher {num}")
            sys.exit(0)
            # break

# {'_id': {'_data': '8266D3EF23000000012B042C0100296E5A10045C1BE20648104131A527B9EAD80E9F75463C6F7065726174696F6E54797065003C696E736572740046646F63756D656E744B65790046645F6964006466D3EF237469C95EDC2C0B49000004'}, 
#  'operationType': 'insert', 
#  'clusterTime': Timestamp(1725165347, 1), 
#  'wallTime': datetime.datetime(2024, 9, 1, 4, 35, 47, 908000), 
#  'fullDocument': {'_id': ObjectId('66d3ef237469c95edc2c0b49'), 'doc': 'doc1', 'body': 'api works for new docs!', 'timestamp': datetime.datetime(2024, 8, 31, 21, 35, 47, 902000)}, 'ns': {'db': 'comments', 'coll': 'doc1'}, 'documentKey': {'_id': ObjectId('66d3ef237469c95edc2c0b49')}}

# # Run the change stream monitor in a separate thread
# change_stream_thread = threading.Thread(target=monitor_changes)
# change_stream_thread.daemon = True
# change_stream_thread.start()

# asyncio.run(monitor_changes(2))
# asyncio.run(monitor_changes(4))

async def main():
    loop = asyncio.get_event_loop()
    # results = await asyncio.gather(monitor_changes(2), monitor_changes(5))
    t1 = asyncio.create_task(monitor_changes(7))
    t2 = asyncio.create_task(monitor_changes(8))
    await t1
    await t2

asyncio.run(main())

# curl -X GET http://127.0.0.1:8000/get_comment -H "Content-Type: application/json"
# curl -X POST http://127.0.0.1:8000/add_comment -H "Content-Type: application/json" -d '{"doc":"doc2","body":"api works for new docs!"}'