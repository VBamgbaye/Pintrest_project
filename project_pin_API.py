from fastapi import FastAPI
from pydantic import BaseModel
import uvicorn
from json import dumps
from kafka import KafkaProducer

app = FastAPI()


class Data(BaseModel):
    category: str
    index: int
    unique_id: str
    title: str
    description: str
    follower_count: str
    tag_list: str
    is_image_or_video: str
    image_src: str
    downloaded: int
    save_location: str


@app.post("/pin/")
def get_db_row(item: Data):
    data = dict(item)

    batch_producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        client_id="Pinterest data producer",
        value_serializer=lambda x: dumps(x).encode("utf-8")
    )
    batch_producer.send(topic="Pinterest_data", value=data)


if __name__ == '__main__':
    uvicorn.run("project_pin_API:app", host="localhost", port=8000)
