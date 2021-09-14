import os
import paho.mqtt.client as mqtt
import time
import random
import json
from dto import Payload

client = mqtt.Client()
client.connect(os.environ.get("BROKER_HOST"), os.environ.get("BROKER_PORT"), 60)

for i in range(100) :
    obj = Payload(str(random.randint(0,11)), 1 if random.randint(0,100) > 30 else 0)
    client.publish("1",obj.serialized())
    time.sleep(1)
    print(obj.serialized())
    
client.loop(2)
