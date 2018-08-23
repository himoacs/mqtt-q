/*******************************************************************************************
/ Author: Thomas Kunnumpurath

/ This module allows users to connect to a message broker via AMQP messaging protocol.
/ It provides q wrapper, via embedPy, on top of python code which uses a python library called proton-amqp.
/ Dependencies include python, proton-amqp and embedPy

/ Examples:
/ To submit a message to a topic:
/ q)amqp_publish["<host>.messaging.solace.cloud:<port>";"topic://test";"TEST";"solace-cloud-client";"<password>"]

/ To receive messages:
/ q)amqp_subscribe["<host>.messaging.solace.cloud:<port>";"topic://test";"solace-cloud-client";"<password>"]
/*******************************************************************************************
p)from __future__ import print_function, unicode_literals
p)from proton.handlers import MessagingHandler
p)from proton.reactor import Container
p)from proton import Message

/ define function which would be called when a connection is established
p)class Recv(MessagingHandler):
    def __init__(self, url, address, count, username, password):
        super(Recv, self).__init__()
        
        # amqp broker host url
        self.url = url

        # amqp node address
        self.address = address

        # authentication credentials
        self.username = username
        self.password = password
        
        # messaging counters
        self.expected = count
        self.received = 0

    def on_start(self, event):
      # select authentication options for connection
        if self.username:
            # basic username and password authentication
            conn = event.container.connect(url=self.url, 
                                           user=self.username, 
                                           password=self.password, 
                                           allow_insecure_mechs=True)
        else:
            # Anonymous authentication
            conn = event.container.connect(url=self.url)
        # create receiver link to consume messages
        if conn:
            event.container.create_receiver(conn, source=self.address)
        print("Connected to AMQP broker!")
        
    def on_message(self, event):
        if event.message.id and event.message.id < self.received:
            # ignore duplicate message
            return
        if self.expected == 0 or self.received < self.expected:
            print(event.message.body)
            self.received += 1
            if self.received == self.expected:
                event.receiver.close()
                event.connection.close()
    
    # the on_transport_error event catches socket and authentication failures
    def on_transport_error(self, event):
        print("Transport error:", event.transport.condition)
        MessagingHandler.on_transport_error(self, event)

    def on_disconnected(self, event):
        print("Disconnected")

p)class Send(MessagingHandler):
    def __init__(self, url, address, messageText, username, password, QoS=1):
        super(Send, self).__init__()
    
        # amqp broker host url
        self.url = url

        # target amqp node address
        self.address = address

        # authentication credentials
        self.username = username
        self.password = password

        self.messageText = messageText

        # the message durability flag must be set to True for persistent messages
        self.message_durability = True if QoS==2 else False

    def on_start(self, event):
        # select connection authenticate
        if self.username:
            # creates and establishes an amqp connection with the user credentials
            conn = event.container.connect(url=self.url, 
                                           user=self.username, 
                                           password = self.password, 
                                           allow_insecure_mechs=True)
        else:
            # creates and establishes an amqp connection with anonymous credentials
            conn = event.container.connect(url=self.url)
        if conn:
            # attaches sender link to transmit messages
            event.container.create_sender(conn, target=self.address)

    def on_sendable(self, event):
        # creates message to send
        msg = Message(body=self.messageText,durable=self.message_durability)
        # sends message
        event.sender.send(msg)

    def on_accepted(self, event):
        print("Message Sent!")
        event.connection.close()

    def on_rejected(self, event):
        print("Broker", self.url, "Reject message:", event.delivery.tag)
        event.connection.close()

    # catches event for socket and authentication failures
    def on_transport_error(self, event):
        print("Transport error:", event.transport.condition)
        MessagingHandler.on_transport_error(self, event)

    def on_disconnected(self, event):
        if event.transport and event.transport.condition :
            print('disconnected with error : ', event.transport.condition)
            event.connection.close()

p)def publish(url,address,messageText,username,password,QoS=1):  
    try:
      # start proton event reactor
      Container(Send(url, address, messageText, username, password, QoS)).run()
    except KeyboardInterrupt: pass       

p)def subscribe(url,address,username,password):
    try:
      Container(Recv(url,address,0,username,password)).run()
    except KeyboardInterrupt: pass


/ link python functions to q functions
amqp_subscribe:.p.get[`subscribe;<]
amqp_publish:.p.get[`publish;<]
