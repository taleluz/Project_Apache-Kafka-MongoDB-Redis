from datetime import datetime
import json
from confluent_kafka import Consumer, KafkaError
from pymongo import MongoClient
import confingfile

mongo_host = confingfile.mongo_host
mongo_port = confingfile.mongo_port
mongo_database = confingfile.mongo_database
mongo_collection = confingfile.mongo_collection

kafka_address = confingfile.kafka_address
kafka_topic = confingfile.kafka_topic
client = MongoClient(f'mongodb://{mongo_host}:{mongo_port}')
db = client[mongo_database]
collection = db[mongo_collection]

conf = {
    'bootstrap.servers': kafka_address,
    'group.id': 'event_consumer',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)
consumer.subscribe([kafka_topic])

# Consume and print events
def consume_events():
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(f"Consumer error: {msg.error()}")
                continue

        event = json.loads(msg.value())
        timestamp_str = event["timestamp"]
        timestamp = datetime.fromisoformat(timestamp_str)
        event["timestamp"] = timestamp
        result = collection.insert_one(event)
        print('Received event:', event)

consume_events()
