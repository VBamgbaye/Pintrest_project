from json import loads
from kafka import KafkaConsumer


def stream_data():
    stream_consumer = KafkaConsumer(
        bootstrap_servers="localhost:9092",
        value_deserializer=lambda x: loads((x.decode("utf-8"))),
        auto_offset_reset="earliest",
        enable_auto_commit=True
    )

    stream_consumer.subscribe(topics=["Pinterest_data"])

    for message in stream_consumer:
        print(message.value)


if __name__ == "__main__":
    stream_data()
