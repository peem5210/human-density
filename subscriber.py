import os
import datetime
import paho.mqtt.client as mqtt
import mysql.connector 
import threading
import pandas as pd
from dotenv import load_dotenv
from dto import Payload


load_dotenv(os.path.join(os.path.dirname('./'), '.env'))

class Main:
    def __init__(self):
        self.state = self.initialize_state()
        self.connection = self.connect_database()
        self.connect_mqtt()
        main.run()
    
    def initialize_state(self):
        df = self.get_sensor_detail()
        return dict.fromkeys(df['sensor_id'].values.tolist())
    
    def run(self) :
        self.insert_total_log(threading.Event())
        self.client.loop_forever()
        
    def connect_database(self):
        return mysql.connector.connect(
            host=os.environ.get("DB_HOST"),
            database=os.environ.get("DB_NAME"),
            user=os.environ.get("DB_USER"),
            password=os.environ.get("DB_PASSWORD")
        )
        
    def connect_mqtt(self):
        self.client = mqtt.Client()
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.connect(os.environ.get("BROKER_HOST"), int(os.environ.get("BROKER_PORT")), 60) 
        
    def insert_msg_log(self, sensor_id, action):
        try : 
            cursor = self.connection.cursor()
            cursor.execute("INSERT INTO message_log (sensor_id, action) VALUES (%s, %s)", (sensor_id, action))
            self.connection.commit()
            cursor.close()
        except mysql.connector.Error as error:
            print("Failed to insert record into Laptop table {}".format(error))

    #The callback for when the client receives a CONNACK response from the server.
    def on_connect(self, client, userdata, flags, rc):
        print(f"Connected with result code {str(rc)}")
        client.subscribe("1")
        
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
        print(f''self.state)
        self.insert_msg_log(obj.sensor_id, obj.action)

    def insert_total_log(self, threading_event):
        try : 
            cursor = self.connection.cursor()
            cursor.execute("INSERT INTO total_log (sensor_id, action) VALUES (%s, %s)", (sensor_id, action))
            self.connection.commit()
            cursor.close()
        except mysql.connector.Error as error:
            print("Failed to insert record into Laptop table {}".format(error))

        if not threading_event.is_set():
            threading.Timer(5, self.insert_total_log, [threading_event]).start()

    def get_sensor_detail(self) :
        detail = pd.read_sql('SELECT * FROM sensor_detail', self.connection)
        print(detail)
      
        

if __name__ == '__main__':
    main = Main()
    main.get_sensor_detail()
    
    
    
    
