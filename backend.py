import os
from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI
from dto import Payload
import paho.mqtt.client as mqtt
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname('./'), '.env'))
app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
client = mqtt.Client()
client.connect(os.environ.get("BROKER_HOST"), int(os.environ.get("BROKER_PORT")), 60)
    
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