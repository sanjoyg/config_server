# Configuration Server
import time
import sys
import threading
from threading import Thread, current_thread
import signal
import getopt
import os
from mqtthelper import MQTTHelper, MQTTCallbacks
import logging
import logging.handlers
import argparse

global q_client
q_client = None

class cs_callback(MQTTCallbacks):
  _config_store_dir = None
  _sys_topic = None
  _response_topic = None 

  def __init__(self,mqtt_client, sys_topic, response_topic, config_store_dir):
  	MQTTCallbacks.__init__(self,mqtt_client)
  	self._config_store_dir = config_store_dir
  	self._sys_topic = sys_topic
  	self._response_topic = response_topic

  def on_message(self,client, userdata, msg):
  	logger.debug("Recieved message : topic<" + msg.topic + "> with payload <" +  msg.payload.decode('utf-8') + ">")
  	try:
  		if msg.topic.find(self._sys_topic) == 0:
  			file_name = msg.payload.decode('utf-8') + ".conf"
  			topic_to_publish_at = self._response_topic.replace('NODE_ID',msg.payload.decode('utf-8'))
  			full_path = os.path.join(self._config_store_dir,file_name)
  			logger.debug("Attempting to spawn thread to read file : <" + full_path + "> and publish to <" + topic_to_publish_at + ">")
  			Thread(target=thread_fx,args=(full_path,self._mqttClient,topic_to_publish_at)).start()
  		else:
  			logger.debug("Ignoring msg as it doesn't start with : " + self._sys_topic)
  	except Exception as ex:
  		# Ignore the exception just dont respond to the msg
  		logger.error("cs_callback::on_message() Got Exception :" + str(ex))
  		logger.error("While parsing msg from topic: <" + msg.topic + "> with payload: <" + msg.payload.decode('utf-8') + ">")
  		
def thread_fx(file_name,q_client,sys_topic):
	config_str = return_sensor_config(file_name)
	if not config_str is None:
		q_client.publish(sys_topic,config_str)
		logger.debug("Published to topic <" + sys_topic + "> contents <" + config_str + ">")
	else:
		logger.warn("Null returned post reading file <" + file_name + ", publishing nothing ")

def return_sensor_config(file_name):
	config = None
	f_handle = None
	try:
		f_handle = open(file_name,'r')
		for line in f_handle:
			line = line.replace('\n','').replace('\r','')
			if config is None:
				config = line
			else:
				config = config + "^" + line
		if config is None:
			logger.error("Empty file <" + file_name + ">")
	except IOError as ioex:
		logger.error("return_sensor_config() Failed to open/read file : " + file_name + ", Exception: " + str(ioex))
		config = None
	except Exception as ex:
		logger.error("return_sensor_config() Unexepected exception opening/reading file : " + file_name + ", Exception: "+ str(ex))
		config = None
	finally:
		if not f_handle is None:
			f_handle.close()
	if config is None:
		logger.debug("Returning read contents from file <" + file_name + "> : Null")
	else:
		logger.debug("Returning read contents from file <" + file_name + "> + <" + config + ">")
	return config
		
def connect_to_q(host,port,user=None,pwd=None,sys_topic=None,response_topic=None,config_store_dir=None):
	q_client = MQTTHelper(host,port,user,pwd,logger=logger)
	if not sys_topic is None:
		q_client.subscribe(sys_topic)
	q_callback = cs_callback(q_client,sys_topic,response_topic,config_store_dir)
	logger.info("Attempting connection to Q")
	q_client.connect_blocking(callback=q_callback)
	logger.info("Connected to Q")
	return q_client

def setup_logger(log_file_name):
	global logger
	logger = logging.getLogger(__name__)
	logger.setLevel(logging.DEBUG)
	handler = logging.handlers.TimedRotatingFileHandler(log_file_name, when="midnight", backupCount=1)
	# Format each log message like this
	formatter = logging.Formatter('%(asctime)s %(levelname)-8s %(message)s')
	# Attach the formatter to the handler
	handler.setFormatter(formatter)
	# Attach the handler to the logger
	logger.addHandler(handler)

def parse_command_line(argv):
	parser = argparse.ArgumentParser("Configuration Server for C503 Sensors")
	parser.add_argument('-s',type=str, dest="host", metavar="<hostname>", help='Q Host Name',required=True)
	parser.add_argument('-n',type=int, dest="port", metavar="<port number>", help='Q Port Number',required=True)
	parser.add_argument('-u',type=str, dest="user", metavar="<q user>", help='Q User Name')
	parser.add_argument('-p',type=str, dest="pwd", metavar="<q password>", help='Q Password')
	parser.add_argument('-c',type=str, dest="config_dir", metavar="<config dir>", help='Configuration Files Directory',required=True)
	parser.add_argument('-t',type=str, dest="sys_topic", metavar="<conf topic>", help='Configuration Q Topic',required=True)
	parser.add_argument('-r',type=str, dest="response_topic", metavar="<response topic template>", help='Configuration Response Q Topic Template',required=True)
	parser.add_argument('-l',type=str, dest="log_file", metavar="<path to log file>", help='Log File Location',required=True)

	args = parser.parse_args(argv)

	return args

def signal_handler(signal, frame):
	global q_client
	logger.info('Pressed Ctrl+C - or killed me with -2, requesting graceful shutdown')
	if not q_client is None:
		q_client.stop()
		time.sleep(2)
	sys.exit(0)

def main(argv):
	global q_client

	signal.signal(signal.SIGINT, signal_handler)
	parsed_args = parse_command_line(argv)
	setup_logger(parsed_args.log_file)

	logger.info("Hail")
	
	q_client = connect_to_q(parsed_args.host,parsed_args.port,parsed_args.user,parsed_args.pwd,parsed_args.sys_topic,parsed_args.response_topic,parsed_args.config_dir)
	q_client.start()
	logger.info("Open for business")
	print("To stop press Ctrl-C")	
	
	while True:
		time.sleep(2);

if __name__ == '__main__':
	main(sys.argv[1:])