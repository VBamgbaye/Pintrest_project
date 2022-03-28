import time
from kafka import KafkaProducer
import json
from create_test_data import get_registered_user


def json_serializer(data):
    return json.dumps(data).encode("utf-8")


TOPIC_NAME = 'registered_user'
KAFKA_SERVER = 'localhost:9092'

producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER, value_serializer=json_serializer)

if __name__ == "__main__":
    while 1 == 1:
        registered_user = get_registered_user()
        print(registered_user)
        producer.send(TOPIC_NAME, registered_user)
        time.sleep(3)
