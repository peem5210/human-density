import paho.mqtt.client as mqtt
import time
import random
import json
from dto import Payload

client = mqtt.Client()
client.connect("localhost", 1883, 60)

for i in range(10) :
    obj = Payload(str(random.randint(0,1)), 1 if random.randint(0,100) > 30 else 0)
    client.publish("1",obj.serialized())
    time.sleep(2)
    print(obj.serialized())
    
client.loop(2)
