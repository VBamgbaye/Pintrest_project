from kafka import KafkaConsumer
from json import loads

TOPIC_NAME = 'MyFirstKafkaTopic'
consumer = KafkaConsumer(TOPIC_NAME, bootstrap_servers=['localhost:9092'],
                         auto_offset_reset='earliest',
                         enable_auto_commit=True,
                         group_id='pin-interest',
                         value_deserializer=lambda x: loads(x.decode('utf-8')))
for message in consumer:
    message = message.value
    # collection.insert_one(message)
    # print('{} added to {}'.format(message, collection))
    print(message)
