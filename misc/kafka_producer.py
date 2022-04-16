import time
from kafka import KafkaProducer
import json
from data_generator import app


def json_serializer(message):
    return json.dumps(message).encode("utf-8")


TOPIC_NAME = 'MyFirstKafkaTopic'
KAFKA_SERVER = 'localhost:9092'

producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER, value_serializer=json_serializer)

if __name__ == "__main__":
    while 1 == 1:
        registered_user = app
        print(registered_user)
        producer.send(TOPIC_NAME, registered_user)
        time.sleep(3)
