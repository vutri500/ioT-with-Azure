import argparse
import config
import json
import paho.mqtt.client as mqtt
from azure.iot.device import IoTHubDeviceClient, Message
from azure.iot.device.exceptions import ConnectionFailedError, ConnectionDroppedError, OperationTimeout, OperationCancelled, NoConnectionError
from log import console, log
import datetime
import sqlite3
import time

mqtt_topic = 'esp32/bme280_data'

parser = argparse.ArgumentParser()
parser.add_argument("connection", nargs='?', help="Device Connection String from Azure",
                    default=config.IOTHUB_DEVICE_CONNECTION_STRING)
parser.add_argument("-t", "--time", type=int, default=config.MESSAGE_TIMESPAN,
                    help="Time in between messages sent to IoT Hub, in milliseconds (default: 2000ms)")
parser.add_argument("-n", "--no-send", action="store_true", 
                    help="Disable sending data to IoTHub, only print to console")
ARGS = parser.parse_args()

device_client = None  # Initialize device_client outside the main function

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print('Connected to MQTT broker!!!')
        client.subscribe(mqtt_topic)
    else:
        print(f'Failed to connect to MQTT broker. Error code: {rc}')

def store_locally(client, userdata, msg):
    try:
        payload = msg.payload.decode('utf-8')
        data = json.loads(payload)
        deviceID = 'ESP32 - BME280'
        temperature = float(data['Temperature'])
        humidity = float(data['Humidity'])
        pressure = float(data['Pressure'])
        timestamp = datetime.datetime.now()

        conn = sqlite3.connect('sensordata.db')
        cursor = conn.cursor()
        cursor.execute(
            """INSERT INTO bme280_data (deviceID, temperature, humidity, pressure, timestamp)
            VALUES(?,?,?,?,?)""", (deviceID, temperature, humidity, pressure, timestamp)
        )
        conn.commit()
        conn.close()
        print(f"Data saved: deviceID={deviceID}, temp={temperature}, hum={humidity}, pres={pressure}, time={timestamp}")

    except Exception as e:
        print(f"Error storing data: {e}")

def send_to_iot_hub(client, userdata, msg):
    try:
        payload = msg.payload.decode('utf-8')
        data = json.loads(payload)
        deviceID = 'ESP32 - BME280'
        temperature = float(data['Temperature'])
        humidity = float(data['Humidity'])
        pressure = float(data['Pressure'])
        rasptimestamp = str(datetime.datetime.now())  # Convert to string

        # Send data to Azure IoT Hub
        message = {
            "deviceId": deviceID,
            "temperature": temperature,
            "humidity": humidity,
            "pressure": pressure,
            "rasptimestamp": rasptimestamp
        }
        send_message(device_client, message)

    except Exception as e:
        print(f"Error processing MQTT message: {e}")

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

def main():
    global device_client  # Add a global declaration

    if not ARGS.connection:  # If no argument
        log.error("IOTHUB_DEVICE_CONNECTION_STRING in config.py variable or argument not found, try supplying one as an argument or setting it in config.py")

    if not ARGS.no_send:
        with console.status("Connecting to IoT Hub with Connection String", spinner="arc", spinner_style="blue"):
            # Create instance of the device client using the connection string
            device_client = IoTHubDeviceClient.create_from_connection_string(ARGS.connection, connection_retry=False)

            try:
                # Connect the device client.
                device_client.connect()
            except Exception as e:
                log.error("Failed to connect to IoT Hub:", e)

        log.success("Connected to IoT Hub")

    mqttc = mqtt.Client()
    mqttc.on_connect = on_connect
    mqttc.connect("localhost", 1883, 60)
    mqttc.loop_start()

    print()  # Blank line

    try:
        while True:
            if ARGS.no_send:
                log.warning('Not sending to IoT Hub - saving to local database')
                # Store locally
                mqttc.on_message = store_locally
            else:
                log.info('Sending data to IoT Hub')
                # Send data to IoT Hub
                mqttc.on_message = send_to_iot_hub

            time.sleep(ARGS.time / 1000)  # Delay between messages

    except KeyboardInterrupt:
        # Shut down the device client when Ctrl+C is pressed
        log.error("Shutting down", exit_after=False)
        device_client.shutdown()


if __name__ == "__main__":
    main()
