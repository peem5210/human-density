import paho.mqtt.client as mqtt
import time

client = mqtt.Client()
client.connect("localhost", 1883, 60)
i = 0
for i in range(10) :
    if i % 3 == 0 :
        i = i - 1
        client.publish("siam_paragon", 0)
    else:
        client.publish("siam_paragon", 1)
    time.sleep(5)
client.loop(2)
