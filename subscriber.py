import paho.mqtt.client as mqtt
import mysql.connector 

connection = mysql.connector.connect(
        host="localhost",
        database="grafanadb",
        user="root",
        password="bob_124578963 "
    )

def post(name, action):
    try : 
        mySql_insert_query = """INSERT INTO log 
                            VALUES 
                            ("{}", {}) """.format(name, action)

        cursor = connection.cursor()
        cursor.execute(mySql_insert_query)
        connection.commit()
        print(cursor.rowcount, "Record inserted successfully into Laptop table")
        cursor.close()
    except mysql.connector.Error as error:
        print("Failed to insert record into Laptop table {}".format(error))

#The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    print("Connected with result code " + str(rc))
    client.subscribe("siam_paragon")
#The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    # print(msg.topic +" " + str(msg.payload))
    if msg.payload == 0 :
        post(msg.topic, msg.payload)
    else:
        post(msg.topic, msg.payload)
client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
client.connect("localhost", 1883, 60)
client.loop_forever()
