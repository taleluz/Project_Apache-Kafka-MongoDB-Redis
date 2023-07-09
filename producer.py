import json
import random
import time
from datetime import datetime
from confluent_kafka import Producer
import confingfile

kafka_address = confingfile.kafka_address
kafka_topic = confingfile.kafka_topic


conf = {
    'bootstrap.servers': kafka_address,
}

producer = Producer(conf)

def generate_event():
    event = {
        'reporterId': random.randint(1, 100000000),
        'timestamp': datetime.now().isoformat(),
        'metricId': random.randint(1, 100000000),
        'metricValue': random.randint(1, 100000000),
        'message': 'Event generated'
    }
    return event

def send_events():
    while True:
        event = generate_event()
        producer.produce(kafka_topic, json.dumps(event))
        producer.flush()
        print('Event sent:', event)
        time.sleep(1)

send_events()
