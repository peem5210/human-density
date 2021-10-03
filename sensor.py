import os
import paho.mqtt.client as mqtt
import time
from dotenv import load_dotenv

from util.dto import Payload
from util.mock_sensor import get_action


SENSOR_ID = 1

load_dotenv(os.path.join(os.path.dirname('./'), '.env'))

#Connect to mqtt client
client = mqtt.Client()
client.connect(os.environ.get("BROKER_HOST"), int(os.environ.get("BROKER_PORT")), 60)

while True:
    #Get action from the sensor
    action = get_action()
    
    #If action is not None, publish to the broker server
    if action != None:
        #Construct payload for sending over network 
        payload = Payload(SENSOR_ID, action)
        
        #Publishing message
        message = payload.serialize()
        client.publish(os.environ.get("ACTION_TOPIC"), message)
        
        print(message)
    
    #Interval simulating as real world data
    time.sleep(1)
    
