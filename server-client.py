# Server-client hybrid

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

myNodeName = ""

waiting_for_response = {} # maps a command to the responses it's received
responses_to_send = {} # maps a node name to a queue of responses it needs to send
key_value_store = {} # maps a key to a (value, src, timestamp)

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

class Listener():
    """
    Class to listen for all incoming messages to this node
    """

    def __init__(self, max_delay, host, port):
        self.host = host
        self.port = port
        self.max_delay = max_delay
        self.message_queue = Queue.Queue()

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
        ts = time.time()
        st = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
        message = pickle.loads(received)

        
        if message.ACK:
            self.process_ACK(message)
        elif message.command == "send":
            print "Received \"" + message.message + "\" from " + message.source + " max delay is " + str(self.max_delay) + " s, system time is " + st
        elif message.command == "insert":
            # TODO: check for case where key already exists
            key_value_store[message.key] = (message.value, message.source, message.time_sent)
            print "inserted key = " + str(message.key) + " value = " + str(message.value)
            message.ACK = True
            responses_to_send[message.source].put(message)
        elif message.command == "update":
            # TODO: check for case where key doesn't exist
            key_value_store[message.key] = (message.value, message.source, message.time_sent)
            print "updated key = " + str(message.key) + " value = " + str(message.value)
            message.ACK = True
            responses_to_send[message.source].put(message)
        elif message.command == "get":
            # TODO: check for case where key doesn't exist
            value = key_value_store[message.key]
            message.ACK = True
            message.value = value[0]
            responses_to_send[message.source].put(message)



    def process_ACK(self, message):
        commands_waiting = waiting_for_response.keys()
        if (message.command == "get"):
            command_key = (message.command, message.key, message.model)
            if command_key in commands_waiting: # TODO: fix this
                waiting_for_response[command_key].append(message.value)
                if int(message.model) == 1:
                    print "get: key = " + str(message.key) + " and value = " + str(message.value)
                    del waiting_for_response[command_key]
                if int(message.model) == 2:
                    # this should never happen
                    print "process_received: Should not receive an ACK on get for sequential consistency!"
                    del waiting_for_response[command_key]
                if int(message.model) == 3:
                    if len(waiting_for_response[command_key]) == 2:
                        print "get: key = " + str(message.key) + " and value = " + str(message.value)
                        del waiting_for_response[command_key]
                if int(message.model) == 4:
                    # if different values were received, the largest will be printed out
                    if len(waiting_for_response[command_key]) == 3:
                        print "get: key = " + str(message.key) + " and value = " + str(max(waiting_for_response[command_key]))
                        del waiting_for_response[command_key]
        else:
            command_key = (message.command, message.key, message.value, message.model)
            if command_key in commands_waiting:
                waiting_for_response[command_key].append("ACK")
                # check to see if command is completed!
                if int(message.model) == 1:
                    del waiting_for_response[command_key]
                    print "command completed"
                if int(message.model) == 2:
                    del waiting_for_response[command_key]
                    print "command completed"
                if int(message.model) == 3:
                    del waiting_for_response[command_key]
                    print "command completed"
                if int(message.model) == 4:
                    # command complete after two ACKs
                    if len(waiting_for_response[command_key]) == 2:
                        del waiting_for_response[command_key]
                        print "command completed"

class CentralListener(Listener):
    """
    Subclass of Listener to operate at Central Server to listen for all incoming messages to this node
    """

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
        message = pickle.dumps(received)
        
        if message.ACK:
            # command completed
            self.process_ACK(message)
        else:
            if message.command == "get":
                value = key_value_store[message.key]
                message.value = value[0]
                message.ACK = True
                responses_to_send[CENTRAL_SERVER_NAME].put(message)

            elif message.command == "insert":
                message.ACK = True
                key_value_store[message.key] = (message.value, message.source, message.time_sent)
                responses_to_send[CENTRAL_SERVER_NAME].put(message)
            elif message.command == "update":
                message.ACK = True
                key_value_store[message.key] = (message.value, message.source, message.time_sent)
                responses_to_send[CENTRAL_SERVER_NAME].put(message)
            else:
                print "CentralListener: invalid command"

    def process_ACK(self, message):
        commands_waiting = waiting_for_response.keys()
        if message.command == "get":
            command_key = (message.command, message.key, message.model)
            if command_key in commands_waiting:
                del waiting_for_response[command_key]
                print "get: key = " + str(message.key) + " value = " + str(message.value)
        else:
            command_key = (message.command, message.key, message.value, message.model)
            if command_key in commands_waiting:
                del waiting_for_response[command_key]
                print "command completed"


class Sender():
    """
    Class to open a connection to another node's Listener and send messages
    """

    def __init__(self, max_delay, src_name, dest_name, host, port):
        self.src_name = src_name
        self.dest_name = dest_name
        self.host = host
        self.port = port
        self.max_delay = max_delay
        self.message_queue = Queue.Queue()

    def start(self):
        senderThread = threading.Thread(target=self.__send)
        senderThread.setDaemon(True)
        senderThread.start()

    def __send(self):
        time.sleep(0.01)
        print "server ready to send on port " + str(self.port)

        while (1):
            if self.message_queue.empty(): # TODO: also check global response queue here
                time.sleep(0.01)
            else:
                message = self.message_queue.get()
                self.execute_command(message)

            if not responses_to_send[self.dest_name].empty():
                response = responses_to_send[self.dest_name].get()
                sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                sock.sendto(pickle.dumps(response), (self.host, self.port))

    def execute_command(self, command):
        delay = random.random() * self.max_delay
        ts = time.time()
        st = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
        if message.command == "send":
            print "Sent \"" + message.key + "\" to " + self.dest_name + ", system time is " + st

        message.time_sent = st
        message.source = self.src_name       
        time.sleep(delay)
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.sendto(pickle.dumps(message), (self.host, self.port))           


class CentralSender(Sender):
    """
    Subclass of Sender to operate at Central Server to send outbound messages
    """

    def __send(self):
        time.sleep(0.01)
        print "Central Server ready to send on port " + str(self.port)

        while (1):

            if self.message_queue.empty(): # TODO: also check global response queue here
                time.sleep(0.01)
            else:
                message = self.message_queue.get()

                delay = random.random() * self.max_delay
                ts = time.time()
                st = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
                message.time_sent = st
                message.source = self.src_name         
                time.sleep(delay)

                sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                sock.sendto(pickle.dumps(message), (self.host, self.port))
                sock.close()

            if responses_to_send[CENTRAL_SERVER_NAME].empty():
                pass
            else:
                message = responses_to_send[CENTRAL_SERVER_NAME].get()
                sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                sock.sendto(pickle.dumps(message), (self.host, self.port))
                sock.close()


# usage: server-client.py conf.txt nodeName
if __name__ == "__main__":
    myNodeName = sys.argv[2]
    config_file = open(sys.argv[1],'r')
    delay_info = config_file.readline()
    max_delay = int(delay_info)
    nodes = {}

    for line in config_file:
        # parse data from line in file
        host, port, nodeName = line.split()

        # store node in dictionary keyed by node name
        nodes[nodeName] = (host, int(port))

    socket.setdefaulttimeout(None)

    # create and start Listener for this node
    listener = Listener(max_delay, *nodes[myNodeName])
    listener.start()
    print "=== Listener Initialized ==="

    # wait for other nodes to be started
    time.sleep(0.05)
    raw_input("Press Enter to launch senders...")

    # create and start senders for this node
    senders = []
    central_sender = None
    for nodeName in nodes:

        if (nodeName == CENTRAL_SERVER_NAME):
            responses_to_send[CENTRAL_SERVER_NAME] = Queue.Queue()
            central_sender = CentralSender(max_delay, myNodeName, nodeName, *nodes[nodeName])
            central_sender.start()

        elif (nodeName != myNodeName):
            responses_to_send[nodeName] = Queue.Queue()
            sender = Sender(max_delay, myNodeName, nodeName, *nodes[nodeName])
            senders.append(sender)
            sender.start()

    print "=== Senders Initialized ==="

    # read commands from stdin until program is terminated
    while(1):
        message = raw_input()
        message_data = message.split()

        ts = time.time()
        st = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')

        if (str(message_data[0]).lower() == "send"):
            for sender in senders:
                if message_data[2] == sender.dest_name:
                    command = Message("send", None, None, None)
                    command.message = message_data[1]
                    sender.message_queue.put(command)

        if (str(message_data[0]).lower() == "insert"):
            message = Message("insert", message_data[1], message_data[2], message_data[3])
            message_keys = key_value_store.keys()

            if message.key in message_keys:
                print "The key you requested already exists."
            else:
                waiting_for_response[(message.command, message.key, message.value, message.model)] = [] # wait for ACK on this command
                key_value_store[message.key] = (message.value, myNodeName, st)
                if int(message.model) == 1:
                    central_sender.message_queue.put(message) # just send to central server
                elif int(message.model) == 2:
                    central_sender.message_queue.put(message) # just send to central server
                elif int(message.model) == 3 or int(message.model) == 4:
                    # send to all neighbor nodes
                    for sender in senders:
                        if sender.dest_name != myNodeName:
                            sender.message_queue.put(message)

        if (str(message_data[0]).lower() == "update"):
            message = Message("update", message_data[1], message_data[2], message_data[3])
            
            message_keys = key_value_store.keys()
            if message.key in message_keys:
                waiting_for_response[(message.command, message.key, message.value, message.model)] = [] # wait for ACK on this command
                key_value_store[message.key] = (message.value, myNodeName, st)
                if int(message.model) == 1:
                    central_sender.message_queue.put(message) # just send to central server
                elif int(message.model) == 2:
                    central_sender.message_queue.put(message) # just send to central server
                elif int(message.model) == 3 or int(message.model) == 4:
                    # send to all neighbor nodes
                    for sender in senders:
                        if sender.dest_name != myNodeName:
                            sender.message_queue.put(message)
            else:
                print "The key you requested does not exist."

        if (str(message_data[0]).lower() == "get"):
            message = Message("get", message_data[1], None, message_data[2])
            waiting_for_response[(message.command, message.key, message.model)] = [] # wait for ACK on this command

            if int(message.model) == 1:
                central_sender.message_queue.put(message)
            elif int(message.model) == 2:
                value = key_value_store[message.key]
                print "get: key = " + str(message.key) + " and value = " + str(value[0])
            elif int(message.model) == 3 or int(message.model) == 4:
                value = key_value_store[message.key]
                waiting_for_response[(message.command, message.key, message.model)].append(value[0])
                for sender in senders:
                    # send to all neighbor nodes
                    sender.message_queue.put(message)

        if (str(message_data[0]).lower() == "show-all"):
            keys = key_value_store.keys()
            for key in keys:
                value = key_value_store[key]
                print "key = " + str(key) + " value = " + str(value[0])
              


