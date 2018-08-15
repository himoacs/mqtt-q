/ This module allows users to connect to a message broker via MQTT messaging protocol.
/ It provides q wrapper, via embedPy, on top of python code which uses a python library called paho-mqtt.
/ Dependencies include python, paho-mqtt and embedPy

/ import paho-mqtt library and time module
p)import paho.mqtt.client as mqttClient
p)import time

/ create a new instance
p)client = mqttClient.Client("message")

/ define function which would be called when a connection is established
p)def on_connect(client, userdata, flags, rc):
     if rc==0:
       print("Connected to broker")
     else:
       print("Connection failed")

/ define function which would be called whenever a message is received
p)def on_message(client, userdata, message):
    print("Message received: "  + message.payload.decode("utf-8"))

/ establish connection to the message broker
p)def connect(host,port,user,pwd,client=client):
    client.username_pw_set(user, password=pwd)          # set username and password
    client.on_connect= on_connect                       # attach function to callback
    client.on_message= on_message                       # attach function to callback
    client.connect(host, port=port)                     # connect to broker
    client.loop_start()

/ subscribe to a topic
p)def subscribe(topic,client=client):
    print("Subscribing to: " + str(topic))
    client.subscribe(topic)

/ publish message to broker
/ paho-mqtt has a publish method as well but here we are using
/ a simple curl command to send the message
publish:{[url;msg;topic;user;pwd]
    url:url,"/",topic;
    cmd:"curl -X POST ",url," -d ","'",msg,"'"," --user ",user,":",pwd;
    system cmd;
 }

/ start the session
p)def start_session(client=client):
    try:
      while True:
        time.sleep(1)

    except KeyboardInterrupt:
      print("exiting")
      client.disconnect()
      client.loop_stop()

/ link python functions to q functions
connect:.p.get[`connect;<]
subscribe:.p.get[`subscribe;<]
start_session:.p.get[`start_session;<]