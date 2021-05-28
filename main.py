# how to start the code
# 1.use the above command to start the kafka server.
#  bin/kafka-server-start.sh config/server.properties
# 2. Start the zookeeper.
# bin/zookeeper-server-start.sh config/zookeeper.properties
# 3. To look for all the kafka topic logs use this command
# bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test --from-beginning
# 4. command to create a kafka topic.
# bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test
# 5. List all the kafka topic.
# bin/kafka-topics.sh --list --zookeeper 10.148.41.13:9092
# cxln1.c.thelab-240901.internal:localhost:9092


# Import KafkaProducer from Kafka library
import sys
import time

from kafka import KafkaProducer
from kafka import KafkaConsumer
import json
from datetime import datetime


def kafka_producer():
    # Initialize producer variable and set parameter for JSON encode
    producer = KafkaProducer(bootstrap_servers='localhost:9092',
                             api_version=(0, 11, 5))

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

    # consume earliest available messages, don't commit offsets
    # KafkaConsumer(auto_offset_reset='earliest', enable_auto_commit=False)

    # consume json messages
    # KafkaConsumer(value_deserializer=lambda m: json.loads(m.decode('ascii')))

    # StopIteration if no message after 1sec
    # KafkaConsumer(consumer_timeout_ms=1000)

    # Subscribe to a regex topic pattern
    # consumer = KafkaConsumer()
    # consumer.subscribe(pattern='%test%')
    # sys.exit()


if __name__ == '__main__':
    kafka_producer()
    # kafka_consumer()
