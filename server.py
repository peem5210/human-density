import os
import paho.mqtt.client as mqtt
import threading
import pandas as pd
from dotenv import load_dotenv
from dto import Payload
from sqlalchemy import create_engine

load_dotenv(os.path.join(os.path.dirname('./'), '.env'))

TOTAL_INTERVAL = 5 #Seconds
DB_CONN_STR = f'mysql+mysqlconnector://{os.environ.get("DB_USER")}:{os.environ.get("DB_PASSWORD")}@{os.environ.get("DB_HOST")}:3306/{os.environ.get("DB_NAME")}'
class Main:
    def __init__(self):
        self.engine = self.connect_engine()
        self.detail_df = self.get_sensor_detail()
        self.sensor_location_mapping = self.all_sensor_dict()
        self.state = self.initialize_state()
        self.connect_mqtt()
        self.run()
    
    def all_sensor_dict(self):
        return dict(self.detail_df[['sensor_id', 'location_id']].values.tolist())
    
    #Initialize empty state for every sensor_id
    def initialize_state(self):
        return dict.fromkeys(self.detail_df['sensor_id'].values.tolist(), 0)
    
    #Start the actual server
    def run(self) :
        self.insert_total_log_loop(threading.Event())
        self.client.loop_forever()
    
    #Connect to database    
    def connect_engine(self):
        print("Connecting db...")
        engine = create_engine(DB_CONN_STR, echo=False)
        print("Connected")
        return engine
    
    #Connect to mqtt broker
    def connect_mqtt(self):
        print("Connecting mqtt...")
        try:
            self.client = mqtt.Client()
            self.client.on_connect = self.on_connect
            self.client.on_message = self.on_message
            self.client.connect(os.environ.get("BROKER_HOST"), int(os.environ.get("BROKER_PORT")), 60) 
        except Exception as e:
            print(e)
        print("Connected")
    
    #Inserting the log recieved from the broker
    def insert_msg_log(self, sensor_id, action):
        try : 
            self.engine.execute("INSERT INTO message_log (sensor_id, action) VALUES (%s, %s)", (sensor_id, action))
        except Exception as error:
            print("Failed to insert record into Laptop table {}".format(error))

    #The callback for when the client receives a CONNACK response from the server.
    def on_connect(self, client, userdata, flags, rc):
        # print(f"Connected with result code {str(rc)}")
        client.subscribe(str(os.environ.get("ACTION_TOPIC")))
        
    #The callback for when a PUBLISH message is received from the server.
    def on_message(self, client, userdata, msg):
        obj = Payload(msg.payload)
        if obj.sensor_id not in self.state:
            print(f'sensor id {obj.sensor_id} not exist in DB')
            self.state[obj.sensor_id] = 0 
        if obj.action == 0 :
            if self.state[obj.sensor_id] <= 0 :
                self.state[obj.sensor_id] = 0
            else :
                self.state[obj.sensor_id] -= 1 
        else:
            self.state[obj.sensor_id] += 1
        # print(f'New state {self.state}')
        self.insert_msg_log(obj.sensor_id, obj.action)
    
    #Inserting total log 
    def insert_total_log(self):
        print("Inserting total log")
        pd.DataFrame(
                data=[[self.sensor_location_mapping[x],y] for x,y in self.state.items()], 
                columns=['location_id', 'total'])\
            .groupby("location_id").sum('total').reset_index()\
            .to_sql('total_log', self.engine, if_exists='append', index=False)

    #Inserting total log every {TOTAL_INTERVAL} interval
    def insert_total_log_loop(self, threading_event):
        self.insert_total_log()
        if not threading_event.is_set():
            threading.Timer(TOTAL_INTERVAL, self.insert_total_log_loop, [threading_event]).start()
    
    #Querying all sensor detail
    def get_sensor_detail(self) :
        detail = pd.read_sql('SELECT * FROM sensor_detail', self.engine)
        return detail
      


if __name__ == '__main__':
    Main()
    
    
    
    
    
