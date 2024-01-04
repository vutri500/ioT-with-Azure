import machine
from machine import Pin, SoftI2C
from umqttsimple import MQTTClient
import esp
import ubinascii
import json
import network
import BME280
import time

esp.osdebug(None)
import gc
gc.collect()

ssid = 'T'
password = '1234567890'
mqtt_server = '192.168.137.40'

client_id = ubinascii.hexlify(machine.unique_id())

topic = 'esp32/bme280_data'

last_message = 0
message_interval = 1

station = network.WLAN(network.STA_IF)

station.active(True)
station.connect(ssid, password)

while station.isconnected() == False:
  pass

print('Connection successful')

# ESP32 - Pin assignment
i2c = SoftI2C(scl=Pin(22), sda=Pin(21), freq=10000)
bme = BME280.BME280(i2c=i2c)


def connect_mqtt():
  global client_id, mqtt_server
  #client = MQTTClient(client_id, mqtt_server)
  client = MQTTClient(client_id, mqtt_server, user='tmqtt', password='minhtri')
  client.connect()
  print('Connected to %s MQTT broker' % (mqtt_server))
  return client

def restart_and_reconnect():
  print('Failed to connect to MQTT broker. Reconnecting...')
  time.sleep(10)
  machine.reset()

def read_bme_sensor():
  try:
    temp = bme.temperature[:-1]
    hum = bme.humidity[:-1]
    pres = bme.pressure[:-3]

    return temp, hum, pres
    #else:
    #  return('Invalid sensor readings.')
  except OSError as e:
    return('Failed to read sensor.')

try:
  client = connect_mqtt()
except OSError as e:
  print(e)
  restart_and_reconnect()

while True:
    try:
        if (time.time() - last_message) > message_interval:
            temp, hum, pres = read_bme_sensor()
            print(temp, hum, pres)
            
            payload = {
                'Temperature': temp,
                'Humidity': hum,
                'Pressure': pres
            }
            
            message = json.dumps(payload)
            client.publish(topic, message)
            last_message = time.time()
            
    except OSError as e:
        restart_and_reconnect()
        