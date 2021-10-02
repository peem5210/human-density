import os
import sys
import time
import random
import paho.mqtt.client as mqtt
from dotenv import load_dotenv

from dto import Payload

load_dotenv(os.path.join(os.path.dirname('./'), '.env'))
client = mqtt.Client()
client.connect(os.environ.get("BROKER_HOST"), int(os.environ.get("BROKER_PORT")), 60)

def get_action():
    rand = random.randint(0, 130)
    if rand < 30:
        return 0
    elif rand < 100:
        return 1
    else:
        return None

if __name__ == '__main__':
    try:
        SENSOR_ID = str(int(sys.argv[1]))
    except Exception as e:
        print(e)
        exit(0)
    
    print(f"\nMocking SENSOR_ID :{SENSOR_ID}\n\n")
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
        
