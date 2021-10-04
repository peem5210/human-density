import os
from dotenv import load_dotenv
import paho.mqtt.client as mqtt

def loadenv():
    load_dotenv(os.path.join(os.path.dirname('./'), '.env'))

def get_mqtt_client():
    client = mqtt.Client()
    client.connect(os.environ.get("BROKER_HOST"), int(os.environ.get("BROKER_PORT")), 60)
    return client
