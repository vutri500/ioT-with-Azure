# Importing necessary libraries
import paho.mqtt.client as mqtt  # MQTT client library
import json  # JSON parsing library
import sqlite3  # SQLite database library
import datetime  # Date and time library

# MQTT topic to subscribe to
mqtt_topic = 'esp32/bme280_data'

def on_connect(client, userdata, flags, rc):
    """
    Callback function executed when the client connects to the MQTT broker.
    It subscribes to the specified MQTT topic.
    """
    print('Connected to MQTT broker!!!')
    client.subscribe(mqtt_topic)

def store_locally(data):
    """
    Function to store sensor data locally in an SQLite database.
    It extracts the relevant data from the received payload and inserts it into the database.
    """
    try:
        # Extracting data from the payload
        deviceID = 'ESP32 - BME280'
        temperature = float(data['Temperature'])
        humidity = float(data['Humidity'])
        pressure = float(data['Pressure'])
        timestamp = datetime.datetime.now()

        # Connecting to the SQLite database
        conn = sqlite3.connect('sensordata.db')
        cursor = conn.cursor()

        # Inserting data into the database
        cursor.execute(
            """INSERT INTO bme280_data (deviceID, temperature, humidity, pressure, timestamp)
            VALUES(?,?,?,?,?)""", (deviceID, temperature, humidity, pressure, timestamp)
        )

        # Committing the changes and closing the database connection
        conn.commit()
        conn.close()

        # Printing the saved data
        print(f"Data saved: deviceID={deviceID}, temp={temperature}, hum={humidity}, pres={pressure}, time={timestamp}")
    except Exception as e:
        print(f"Error storing data: {e}")

def on_message(client, userdata, msg):
    """
    Callback function executed when a new MQTT message is received.
    It decodes the payload, parses it as JSON, and calls the store_locally function.
    """
    try:
        payload = msg.payload.decode('utf-8')
        data = json.loads(payload)
        store_locally(data)
    except Exception as e:
        print(f"Error processing MQTT message: {e}")

# Creating an MQTT client instance
mqttc = mqtt.Client()

# Assigning callback functions
mqttc.on_connect = on_connect
mqttc.on_message = on_message

# Connecting to the MQTT broker
mqttc.connect("localhost", 1883, 60)

# Starting the MQTT network loop in a separate thread
mqttc.loop_start()
