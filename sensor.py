import os
import paho.mqtt.client as mqtt
import time
from dotenv import load_dotenv

from dto import Payload
from mock_sensor import get_action


SENSOR_ID = 1

load_dotenv(os.path.join(os.path.dirname('./'), '.env'))
client = mqtt.Client()
client.connect(os.environ.get("BROKER_HOST"), int(os.environ.get("BROKER_PORT")), 60)

while True:
    action = get_action()
    if action != None:
        #Construct payload for sending over network 
        payload = Payload(SENSOR_ID, action)
        
        #Publishing message
        message = payload.serialize()
        client.publish(os.environ.get("ACTION_TOPIC"), message)
        print(message)
    time.sleep(1)
    
