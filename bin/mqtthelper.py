import paho.mqtt.client as paho
import time
import logging

class MQTTCallbacks:
  _mqttClient = None
  _logger = None

  def __init__(self,mqttClient):
    self._mqttClient = mqttClient
    self._logger = mqttClient._logger

  def on_connect(self, client, userdata, flags, rc):
    time.sleep(1)
    self._mqttClient.set_connected(True)
    for topic in self._mqttClient.get_subscriptions():
      if not self._logger is None: 
        self._logger.debug("Subscribing to topic: " + topic)
      result = self._mqttClient._mqttClient.subscribe(topic)    
    if not self._logger is None:
      self._logger.debug("Connected with result code "+str(rc))

  def on_disconnect(self, client,userdata,rc):
    self._connected = False
    if not self._logger is None:
      self._logger.info("Disconnected from q")

  # The callback for when a PUBLISH message is received from the server.
  def on_message(self,client, userdata, msg):
    if not self._logger is None:
      self._logger.debug(msg.topic+" "+str(msg.payload))

  #def on_log(self, client, userdata, level, buf):
  #  if not self._logger is None:
  #    self._logger.debug("log: ",str(buf))


class MQTTHelper:
  _connected = False
  _mqttClient = None
  _user = None
  _pwd = None
  _host = None
  _port = 0
  _subscriptionList = []
  _callback = None
  _logger = None

  def __init__(self, host, port, user=None, pwd=None, logger=None):
    self._connected = False
    self._mqttClient = None
    self._host = host
    self._port = port
    self._user = user
    self._pwd = pwd
    self._logger = logger 
 
  def set_logger(logger):
    _logger = logger

  def subscribe(self,topic):
    if not topic in self._subscriptionList:
      self._subscriptionList.append(topic)

  def get_subscriptions(self):
    return self._subscriptionList

  def isConnected(self):
  	return self._connected;
  
  def set_connected(self,is_connected):
    self._connected = is_connected


  def connect(self, callback=None, userData=None):
    self._mqttClient = paho.Client(userdata=userData)
    self._callback = callback
    
    if callback is None:
      self._callback = MQTTCallbacks(self)

    self._mqttClient.on_connect = self._callback.on_connect
    self._mqttClient.on_message = self._callback.on_message
    self._mqttClient.on_disconnect = self._callback.on_disconnect
    #self._mqttClient.on_log = self._callback.on_log
    
    if not self._user is None:
      self._mqttClient.username_pw_set(self._user,self._pwd)
    try: 
      self._mqttClient.connect(self._host,self._port,60)	
    except Exception as ex:
      if not self._logger is None:
        self._logger.error("MQTTHelper::connect() : Encountered Exception while connecting : ", str(ex))
      else:
        print("MQTTHelper::connect() : Encountered Exception while connecting : ", str(ex))

  def connect_blocking(self,callback=None,userData=None):
    while not self.isConnected():
      self.stop()
      self.connect(callback,userData)
      self.start()
      retry_count = 0
      while retry_count < 10 and not self.isConnected():
        time.sleep(2)
        retry_count = retry_count + 1
      if not self.isConnected():
        if not self._logger is None:
          self._logger.error("MQTTHelper::connect_blocking() Failed to connect to MQ, attempting again...")
        else:
          print("MQTTHelper::connect_blocking() Failed to connect to MQ, attempting again...")

  def start(self):
  	if not self._mqttClient is None:
  		self._mqttClient.loop_start()

  def stop(self):
  	if not self._mqttClient is None:
  		self._mqttClient.disconnect()
  		self._mqttClient.loop_stop()

  def publish(self,q,payload):
    if not self._mqttClient is None:
    	self._mqttClient.publish(q,payload)