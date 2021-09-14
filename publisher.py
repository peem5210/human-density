import os
import paho.mqtt.client as mqtt
import time
import random
import json
from dotenv import load_dotenv
from dto import Payload

load_dotenv(os.path.join(os.path.dirname('./'), '.env'))

client = mqtt.Client()
client.connect("54.169.177.53", 1883, 60)

for i in range(100) :
    obj = Payload(str(random.randint(0,11)), 1 if random.randint(0,100) > 30 else 0)
    client.publish("1",obj.serialized())
    time.sleep(1)
    print(obj.serialized())
    
client.loop(2)
