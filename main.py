import sys
import time

from kafka import KafkaProducer
from kafka import KafkaConsumer
import json
from datetime import datetime


def kafka_producer():
    # Initialize producer variable and set parameter for JSON encode
    producer = KafkaProducer(bootstrap_servers='localhost:9092',
                             api_version=(0, 10))

    # Send data in JSON format
    file_location = "/home/nineleaps/Downloads/people.json"
    key1 = bytes(str(datetime.now().date()), encoding='utf-8')
    with open(file_location) as f:
        data = json.load(f)
        count = 0
        for i in data:
            if count == 5:
                kafka_consumer()
                time.sleep(1)
                count = 0
            i['time_stamp'] = str(datetime.now())
            logs = json.dumps(i)
            payload = bytes(logs, encoding='utf-8')
            producer.send("test1", value=payload, key=key1)
            print("Messege sent")
            count = count + 1
    f.close()
    print("Message Sent to Kafka ")
    producer.flush()


def kafka_consumer():
    # Initialize consumer variable and set property for JSON decode
    # To consume latest messages and auto-commit offsets
    consumer = KafkaConsumer('test1', auto_offset_reset='earliest',
                             api_version=(0, 10), consumer_timeout_ms=1000,
                             bootstrap_servers=['localhost:9092'])
    count = 0
    for message in consumer:
        count = count + 1
        if count == 5:
            consumer.close()
        print("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                             message.offset, message.key,
                                             message.value))

    return print("Received Message")

if __name__ == '__main__':
    kafka_producer()
    # kafka_consumer()
