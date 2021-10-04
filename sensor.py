import os
import time

from hdensity.dto.payload import Payload
from hdensity.mock.mock_sensor import get_action
from hdensity.util.util_func import *

SENSOR_ID = os.environ.get("SENSOR_ID")


#Connect to mqtt client
loadenv()
client = get_mqtt_client()


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
    
