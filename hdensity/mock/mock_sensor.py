import os
import sys
import time
import random
import paho.mqtt.client as mqtt

from hdensity.dto.payload import Payload

def get_action():
    rand = random.randint(0, 130)
    if rand < 30:
        return 0
    elif rand < 100:
        return 1
    else:
        return None

def mock_all_sensor(client):
    print(f"\nMocking ALL SENSOR\n\n")
    while True:
        obj = Payload(str(random.randint(0, 44)), 1 if random.randint(0, 100) > 30 else 0)
        client.publish(str(os.environ.get("ACTION_TOPIC")), obj.serialize())
        print(obj.serialize())
        time.sleep(1)

def mock_sensor(client, SENSOR_ID):
    print(f"\nMocking SENSOR_ID :{SENSOR_ID}\n\n")
    while True:
        action = get_action()
        if action != None:
            payload = Payload(SENSOR_ID, action)
            message = payload.serialize()
            client.publish(os.environ.get("ACTION_TOPIC"), message)
            print(message)
        time.sleep(1)
        

            
    

        
