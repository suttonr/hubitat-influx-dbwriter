import os
import json
import paho.mqtt.client as mqtt
import datetime
import requests

# Get Env Var Settings
MQTT_HOST = os.environ["DBWRITER_MQTT_HOST"]
MQTT_USER = os.environ["DBWRITER_MQTT_USER"]
MQTT_PASS = os.environ["DBWRITER_MQTT_PASS"]
MQTT_TOPIC = os.environ["DBWRITER_MQTT_TOPIC"]
DB_HOST = os.environ["DBWRITER_DB_HOST"]
#DB_USER = os.environ["DBWRITER_DB_USER"]
#DB_PASS = os.environ["DBWRITER_DB_PASS"]
DB_DB = os.environ["DBWRITER_DB_DB"]
#DB_TABLE = os.environ["DBWRITER_DB_TABLE"]


def send_to_influx(measure):
  url = f'http://{DB_HOST}:8086/write?db={DB_DB}'
  x = requests.post(url, data = measure)

# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))

    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    client.subscribe(MQTT_TOPIC)

def build_metric(topic,msg):
  tags = {}
  value = msg

  tags['device'] = topic[2].replace(" ","_")
  if topic[3] == 'pushed':
    tags['button'] = msg
    value=1
  elif topic[3] == 'released':
    tags['button'] = msg
    value=0

  if (msg == 'active' or 
      msg == 'on' or 
      msg == 'wet' or
      msg == 'heating' or 
      msg == 'open'):
    value='state=1'
  elif (msg == 'inactive' or
        msg =='off' or 
        msg == 'dry' or
        msg == 'idle' or  
        msg == 'closed'):
    value='state=0'
  elif topic[3]=='temperature':
    value=f'temp={msg}'
  else:
    value=f'value={msg}'
  return (tags,value) 

# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    tstamp=datetime.datetime.now().strftime('%s%f')
    topic=msg.topic.split('/')
    measure=topic[3].replace(" ","_")
    (tags,value) = build_metric(topic,msg.payload.decode("utf-8"))
    tags_s=",".join('{}={}'.format(k,v) for k,v in tags.items())
    m=f'{measure},{tags_s} {value} {tstamp}000'
    print(m)
    send_to_influx(m)

def on_subscribe(client, userdata, mid, granted_qos):
    print(f"subscribed to {userdata} {mid} {granted_qos}")

def main():
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message
    client.on_subscribe = on_subscribe

    if MQTT_USER:
        if MQTT_PASS:
            client.username_pw_set(MQTT_USER, password=MQTT_PASS)
        else:
            client.username_pw_set(MQTT_USER, password=None)
    client.connect(MQTT_HOST, 1883, 60)

    # Blocking call that processes network traffic, dispatches callbacks and
    # handles reconnecting.
    # Other loop*() functions are available that give a threaded interface and a
    # manual interface.
    client.loop_forever()

if __name__ == '__main__':
    main()
