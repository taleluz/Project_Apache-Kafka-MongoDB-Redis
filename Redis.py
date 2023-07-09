import threading
import time
import json
from pymongo import MongoClient
import redis
from datetime import datetime
import confingfile

mongo_host = confingfile.mongo_host
mongo_port = confingfile.mongo_port
mongo_database = confingfile.mongo_database
mongo_collection = confingfile.mongo_collection

redis_host = confingfile.redis_host
redis_port = confingfile.redis_port


def copy_data():
    mongo_client = MongoClient(host=mongo_host, port=mongo_port)
    mongo_db = mongo_client[mongo_database]
    mongo_coll = mongo_db[mongo_collection]
    print("Create MongoDB connection")

    redis_client = redis.StrictRedis(host=redis_host, port=redis_port)
    print("Create Redis connection")

    last_copied_timestamp = redis_client.get('last_copied_timestamp')
    if last_copied_timestamp is None:
        last_copied_timestamp = datetime.now()
        redis_client.set('last_copied_timestamp', str(last_copied_timestamp))
        return

    last_copied_timestamp = datetime.strptime(last_copied_timestamp.decode('utf-8'), '%Y-%m-%d %H:%M:%S.%f')

    query = {'timestamp': {'$gt': last_copied_timestamp}}
    cursor = mongo_coll.find(query)

    for document in cursor:
        reporter_id = document['reporterId']
        timestamp = document['timestamp']

        key = f"{reporter_id}:{timestamp}"
        document['timestamp'] = str(document['timestamp'])
        document['_id'] = str(document['_id'])
        value = json.dumps(document)
        redis_client.set(key, value)
        print(key)

    if timestamp:
        redis_client.set('last_copied_timestamp', str(timestamp))
        print("Update the last_copied_timestamp")
        print("Current number of keys:", len(redis_client.keys()))

    mongo_client.close()
    redis_client.close()

def main():
    while True:
        copy_thread = threading.Thread(target=copy_data)
        copy_thread.start()

        copy_thread.join()

        time.sleep(30)

if __name__ == '__main__':
    main()
