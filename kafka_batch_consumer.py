import json
import os.path
from json import loads
from uuid import uuid4

from kafka import KafkaConsumer

consumer = KafkaConsumer(
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda x: loads(x),
    group_id='Pintrestdata_{}'.format(uuid4()),
    auto_offset_reset="earliest",
    # max_poll_records=5,
    enable_auto_commit=True
)
consumer.subscribe(topics=["Pinterest_data"])


def batch_consumer():
    for message in consumer:
        batch_message = message.value
        i = 0
        while os.path.exists(f'batch_data{i}.json'):
            i += 1
        with open(f'batch_data{i}.json', 'w') as file:
            json.dump(batch_message, file, indent=4)


def get_messages(num_messages_to_consume):
    messages = []
    while len(messages) < num_messages_to_consume:
        record = next(consumer)
        line = record.value
        messages.append(line)
    consumer.commit()
    return messages


def save_messages():
    i = 0
    while os.path.exists(f'batch_data{i}.json'):
        i += 1
    with open(f'batch_data{i}.json', 'w') as file:
        json.dump(get_messages(10), file, indent=4)


if __name__ == '__main__':
    batch_consumer()
