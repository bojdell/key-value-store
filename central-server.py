import SocketServer
import socket
import sys
import threading
import Queue
import time
import datetime
import random
import pickle

CENTRAL_SERVER_NAME = "CENTRAL"

messages_to_send = {}
waiting_for_response = {}
nodeNames = []

class Message():

	def __init__(self, command, key, value, model):
		self.command = command
		self.key = key
		self.value = value
		self.model = model
		self.source = None
		self.time_sent = None
		self.ACK = False
		self.message = None

class CentralListener():

	def __init__(self, host, port):
		self.host = host
		self.port = port

	def start(self):
		listenerThread = threading.Thread(target=self.__listen)
		listenerThread.setDaemon(True)
		listenerThread.start()


	def __listen(self):
		# Create a listener that will receive all incoming messages to this node
		sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
		sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		sock.settimeout(None)
		sock.bind((self.host, self.port))

		while(1):
			# Receive data from the server
			received, addr = sock.recvfrom(1024)
			time.sleep(0.01)
			if received:
				self.process_received(received)

	def process_received(self, received):
		message = pickle.loads(received)
		if message.ACK:
			self.process_ACK(message)
		else:
			if message.command == "get":
				command_key = (message.command, message.key, message.model, message.source)
				waiting_for_response[command_key] = []
				for node in nodeNames:
					if node != message.source:
						messages_to_send[node].put(message)

			elif message.command == "insert" or message.command == "update":
				command_key = (message.command, message. key, message.value, message.model, message.source)
				waiting_for_response[command_key] = []
				for node in nodeNames:
					if node != message.source:
						messages_to_send[node].put(message)

	def process_ACK(self, message):
		commands_waiting = waiting_for_response.keys()
		if message.command == "get":
			command_key = (message.command, message.key, message.model, message.source)

			if command_key in commands_waiting:
				waiting_for_response[command_key].append(message.value)

			if int(message.model) == 1:
				if len(waiting_for_response[command_key]) == 3: 
					# we've received all responses
					message.value = max(waiting_for_response[command_key])
					message.ACK = True
					messages_to_send[message.source].put(message)
				else:
					print "invalid model"
		else:
			command_key = (message.command, message.key, message.value, message.model, message.source)

			if command_key in commands_waiting:
				waiting_for_response[command_key].append("ACK")

			if int(message.model) == 1 or int(message.model) == 2:
				if len(waiting_for_response[command_key]) == 3:
					# we've received all responses
					message.ACK = True
					messages_to_send[message.source].put(message)
			else:
				print "invalid model"


class CentralSender(): # this will set up sender sockets for all the nodes

	def __init__(self, dest_name, host, port):
		self.host = host
		self.port = port
		self.src_name = CENTRAL_SERVER_NAME
		self.dest_name = dest_name

	def start(self):
		senderThread = threading.Thread(target=self.__send)
		senderThread.setDaemon(True)
		senderThread.start()

	def __send(self):
		# check the messages_to_send queue and fire them off
		# if dest node matches your name
		while 1:
			if messages_to_send[self.dest_name].empty():
				pass
			else:
				message = messages_to_send[self.dest_name].get()
				sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
				sock.sendto(pickle.dumps(message), (self.host, self.port))


# usage: central-server.py conf.txt
if __name__ == "__main__":
	config_file = open(sys.argv[1],'r')
	delay_info = config_file.readline()
	nodes = {}

	for line in config_file:
		# parse data from line in file
		host, port, nodeName = line.split()
		# store node in dictionary keyed by node name
		nodes[nodeName] = (host, int(port))

	socket.setdefaulttimeout(None)

	# create and start Listener for this node
	listener = CentralListener(*nodes[CENTRAL_SERVER_NAME])
	listener.start()
	print "=== Listener Initialized ==="

	# wait for other nodes to be started
	time.sleep(0.05)
	raw_input("Press Enter to launch senders...")

	# create and start senders for this node
	senders = []

	for nodeName in nodes:
		if(nodeName != CENTRAL_SERVER_NAME):
			nodeNames.append(nodeName)
			messages_to_send[nodeName] = Queue.Queue()
			sender = CentralSender(nodeName, *nodes[nodeName])
			senders.append(sender)
			sender.start()

	print "=== Senders Initialized ==="

	while(1):
		pass




