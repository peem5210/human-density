import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware


from hdensity.dto.payload import Payload
from hdensity.util.util_func import *


loadenv()
client = get_mqtt_client()
app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/in/{sensor_id}")
def check_in(
    sensor_id: int,
    ):
    client.publish(os.environ.get("ACTION_TOPIC"), Payload(sensor_id, 1).serialize())
    return "Success"

@app.get("/out/{sensor_id}")
def check_in(
    sensor_id: int,
    ):
    client.publish(os.environ.get("ACTION_TOPIC"), Payload(sensor_id, 0).serialize())
    return "Success"