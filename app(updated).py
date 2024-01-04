import argparse
import config
import json
import paho.mqtt.client as mqtt
from azure.iot.device import IoTHubDeviceClient, Message
from azure.iot.device.exceptions import ConnectionFailedError, ConnectionDroppedError, OperationTimeout, OperationCancelled, NoConnectionError
from log import console, log  # Assuming you have a custom log module
import datetime
import sqlite3
import time

mqtt_topic = 'esp32/bme280_data'

# Parsing command-line arguments
parser = argparse.ArgumentParser()
parser.add_argument("connection", nargs='?', help="Device Connection String from Azure",
                    default=config.IOTHUB_DEVICE_CONNECTION_STRING)
parser.add_argument("-t", "--time", type=int, default=config.MESSAGE_TIMESPAN,
                    help="Time in between messages sent to IoT Hub, in milliseconds (default: 2000ms)")
parser.add_argument("-n", "--no-connection", action="store_true", 
                    help="Disable sending data to IoTHub")
ARGS = parser.parse_args()

device_client = None  # Initialize device_client outside the main function

# MQTT client callback when connection to the broker is established
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        log.success('Connected to MQTT broker!')
        client.subscribe(mqtt_topic)
    else:
        print(f'Failed to connect to MQTT broker. Error code: {rc}')

# Callback function to store data locally
def store_locally(client, userdata, msg):
    try:
        payload = msg.payload.decode('utf-8')
        data = json.loads(payload)
        deviceID = 'pi'
        temperature = float(data['Temperature'])
        humidity = float(data['Humidity'])
        pressure = float(data['Pressure'])
        rasptimestamp = datetime.datetime.now()
        status = 'Pending'

        # Store data in SQLite database
        conn = sqlite3.connect('sensordata.db')
        c = conn.cursor()
        c.execute(
            """INSERT INTO pending_data (deviceID, temperature, humidity, pressure, rasptimestamp, status)
            VALUES(?,?,?,?,?,?)""", (deviceID, temperature, humidity, pressure, rasptimestamp, status)
        )
        conn.commit()
        conn.close()

        log.warning(f"Pending: deviceID={deviceID}, temp={temperature}, hum={humidity}, pres={pressure}, time={rasptimestamp}")

    except Exception as e:
        print(f"Error storing data: {e}")

# Callback function to send data to IoT Hub
def send_to_iot_hub():
    conn = sqlite3.connect('sensordata.db')
    c = conn.cursor()
    try:
        messages = c.execute("SELECT * FROM pending_data WHERE status = 'Pending'").fetchall()
        for message in messages:
            try:
                log.sending("Sending message to Iot Hub")
                msg = {
                    "deviceId": message[0],
                    "temperature": message[1],
                    "humidity": message[2],
                    "pressure": message[3],
                    "rasptimestamp": message[4]
                }
                send_message(device_client, msg)
                c.execute("UPDATE pending_data SET status ='sent' WHERE deviceID=? AND rasptimestamp=?",(message[0], message[4]))                    
                conn.commit()
            except Exception as e:
                print(f"Error sending message: {e}")
    except Exception as e: 
        print(f"Error sending message to Iot Hub: {e}")
    conn.close()


# Function to send a message to IoT Hub
def send_message(client, message):
    telemetry = Message(json.dumps(message))
    telemetry.content_encoding = "utf-8"
    telemetry.content_type = "application/json"

    try:
        client.send_message(telemetry)
    except (ConnectionFailedError, ConnectionDroppedError, OperationTimeout, OperationCancelled, NoConnectionError):
        log.warning("Message failed to send, skipping")
    else:
        log.success("Message successfully sent!", message)

# Main function
def main():
    global device_client  # Add a global declaration

    mqttc = mqtt.Client()
    mqttc.username_pw_set('tmqtt','minhtri')
    mqttc.on_connect = on_connect
    mqttc.connect("localhost", 1883, 60)
    mqttc.loop_start()

    if not ARGS.connection:  # If no connection string provided
        log.error("IOTHUB_DEVICE_CONNECTION_STRING in config.py variable or argument not found, try supplying one as an argument or setting it in config.py")

    with console.status("Connecting to IoT Hub with Connection String", spinner="arc", spinner_style="blue"):
        # Create instance of the device client using the connection string
        device_client = IoTHubDeviceClient.create_from_connection_string(ARGS.connection, connection_retry=False)

        try:
            # Connect the device client.
            device_client.connect()
        except Exception as e:
            log.error("Failed to connect to IoT Hub:", e)

        log.success("Connected to IoT Hub")


    try:
        while True:
            with console.status("Sending message to IoTHub...", spinner="bouncingBar"):
                # Send data to IoT Hub
                mqttc.on_message = store_locally
                send_to_iot_hub()

            with console.status(f"Waiting {ARGS.time}ms...", spinner_style="blue"):
                time.sleep(ARGS.time / 1000)  # Delay between messages
                
    except KeyboardInterrupt:
        # Shut down the device client when Ctrl+C is pressed
        log.error("Shutting down", exit_after=False)
        device_client.shutdown()

if __name__ == "__main__":
    main()
