import os
import paho.mqtt.client as mqtt
import time
import random
from dotenv import load_dotenv
from dto import Payload

load_dotenv(os.path.join(os.path.dirname('./'), '.env'))

client = mqtt.Client()
client.connect(os.environ.get("BROKER_HOST"), int(os.environ.get("BROKER_PORT")), 60)

for i in range(1000) :
    obj = Payload(str(random.randint(0, 44)), 1 if random.randint(0, 100) > 30 else 0)
    client.publish(str(os.environ.get("ACTION_TOPIC")), obj.serialize())
    time.sleep(1)
    print(obj.serialize())
    
client.loop(2)
