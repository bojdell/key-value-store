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
from operator import itemgetter # for sorting tuples by timestamp in inconsistency repair

CENTRAL_SERVER_NAME = "CENTRAL"

myNodeName = ""
currentCommand = None
acksReceived = []

responses_to_send = {} # maps a node name to a queue of responses it needs to send
key_value_store = {} # maps a key to a (value, src, timestamp)

class Message():

    def __init__(self, command, key, value, model):
        self.command = str(command).lower() if command else None
        self.key = int(key) if key else None
        self.value = int(value) if value else None
        self.model = int(model) if model else None
        self.source = None
        self.time_sent = None
        self.ACK = False
        self.message = None

    def __str__(self):
        result = ""
        if self.command:
            result += "command: " + self.command + " "
        if self.key:
            result += str(self.key) + " "
        if self.value:
            result += str(self.value) + " "
        if self.model:
            result += str(self.model) + " "
        if self.source:
            result += "from: " + self.source + " "
        if self.time_sent:
            result += "time_sent: " + str(self.time_sent) + " "
        if self.ACK:
            result += "ACK: " + str(self.ACK) + " "
        if self.message:
            result += "message: " + self.message + " "
        return result.strip()

# returns a timestamp string
def timestamp():
    ts = time.time()
    return datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')

class Listener():
    """
    Class to listen for all incoming messages to this node
    """

    def __init__(self, max_delay, host, port):
        self.host = host
        self.port = port
        self.max_delay = max_delay

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
        st = timestamp()
        message = pickle.loads(received)

        # print "message: " + str(message)    # DEBUG
        
        # if this response is an ack, process it
        if message.ACK:
            self.process_ACK(message)

        # else, if it was a send command, simply print out the message
        elif message.command == "send":
            print "Received \"" + message.message + "\" from " + message.source + " max delay is " + str(self.max_delay) + " s, system time is " + st

        elif message.command == "delete":
            keys = key_value_store.keys()
            if message.key in keys:
                del key_value_store[message.key]
            else:
                print "This key doesn't exist, key = " + str(message.key)

        else:
            # print "Received command \"" + str(message) + "\"" #DEBUG

            keys = key_value_store.keys()
            # parse and perform command
            if message.command == "insert":
                if message.key in keys:
                    print "This key already exists, key = " + str(message.key)
                else:
                    key_value_store[message.key] = (message.value, message.source, message.time_sent)
                    print "inserted key = " + str(message.key) + " value = " + str(message.value)

            elif message.command == "update":
                if message.key in keys:
                    value = key_value_store[message.key]
                    ts_curr = time.strptime(value[2], "%Y-%m-%d %H:%M:%S")
                    ts_next = time.strptime(message.time_sent, "%Y-%m-%d %H:%M:%S")
                    if ts_next > ts_curr:
                        key_value_store[message.key] = (message.value, message.source, message.time_sent)
                        print "updated key = " + str(message.key) + " value = " + str(message.value)
                else:
                    print "This key doesn't exist, key = " + str(message.key)
            
            elif message.command == "get":
                # TODO: check for case where key doesn't exist
                # add all data to message so can compare timestamp
                data = key_value_store[message.key]
                message.message = data

            elif message.command == "search":
                message.value = myNodeName
                if message.key in keys:
                    message.message = "YES"
                else:
                    message.message = "NO"

            # send ack
            message.ACK = True

            # place ack in proper response queue
            if message.model == 1 or message.model == 2:
                responses_to_send[CENTRAL_SERVER_NAME].put(message)
            else:   
                responses_to_send[message.source].put(message)
            # print "sent message : " + str(message) + " to " + old_source    # DEBUG

    def process_ACK(self, message):
        # print "currentCommand: " + str(currentCommand)    # DEBUG
        if (message.command == "get"):
            command_key = (message.command, message.key, message.model)
            # print "command_key: " + str(command_key)    # DEBUG
            # print "acks received: " + str(acksReceived)    # DEBUG
            # print "len of acks recvd " + str(len(acksReceived)) # DEBUG

            # if this ack is for our current command, process it. else, ignore it
            if command_key == currentCommand:
                # append the data stored in message field, so we can sort by timestamp
                acksReceived.append(message.message)

                #print "ACK " + str(len(acksReceived)) + " received value = " + message.value
        elif (message.command == "search"):
            command_key = (message.command, message.key)
            if command_key == currentCommand:
                acksReceived.append("ACK")
                if message.message == "YES":
                    print message.value
        else:
            command_key = (message.command, message.key, message.value, message.model)
            # print "command_key: " + str(command_key) # DEBUG
            # print "acks received: " + str(acksReceived)    # DEBUG
            # print "len of acks recvd " + str(len(acksReceived)) # DEBUG
            
            # if this ack is for our current command, process it. else, ignore it
            if command_key == currentCommand:
                acksReceived.append("ACK")
                #print "ACK " + str(len(acksReceived)) + " received"

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
                # print "got message from message queue"    # DEBUG
                message = self.message_queue.get()
                self.execute_command(message)

            if not responses_to_send[self.dest_name].empty():
                response = responses_to_send[self.dest_name].get()
                if response.command == "search":
                    pass
                else:
                    delay = random.random() * self.max_delay
                    time.sleep(delay)

                sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                sock.sendto(pickle.dumps(response), (self.host, self.port))

    def execute_command(self, message):
        if message.command == "search":
            pass
        else:
            delay = random.random() * self.max_delay
            ts = time.time()
            st = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
            if message.message == "send":
                print "Sent \"" + message.key + "\" to " + self.dest_name + ", system time is " + st

            message.time_sent = st
            message.source = self.src_name       
            time.sleep(delay)

        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.sendto(pickle.dumps(message), (self.host, self.port))      


class CentralSender(Sender):
    """
    Subclass of Sender to send messages to a Central Server
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

# inserts a value into the key value store, with overwrites
def insertValue(message):
    # add timestamp to message
    message.time_sent = timestamp()

    # if we are using linearizability or seq. consistency, send this command to the central server
    if message.model == 1 or message.model == 2:
        numAcksNeeded = 1
        central_sender.message_queue.put(message)

        while len(acksReceived) < 1:
            time.sleep(0.1)
        key_value_store[message.key] = (message.value, myNodeName, st)
    # else, we need to wait for acks
    elif message.model == 3 or message.model == 4:
        numAcksNeeded = message.model - 2
        key_value_store[message.key] = (message.value, myNodeName, message.time_sent)
        # send command to all neighbor nodes
        for sender in senders:
            if sender.dest_name != myNodeName:
                sender.message_queue.put(message)

        # print "Sent command \"" + str(message) + "\", waiting for " + str(numAcksNeeded) + " acks" #DEBUG
        # print "acks recvd " + str(acksReceived) #DEBUG
        # print "len of acks recvd " + str(len(acksReceived)) #DEBUG

        # wait to receive enough acks
        while len(acksReceived) < numAcksNeeded:
            # print "acks recvd " + str(acksReceived) #DEBUG
            # print "len of acks recvd " + str(len(acksReceived)) #DEBUG
            time.sleep(0.05)

    # once we have enough acks, print result and proceed to read in a new command
    if message.command == "insert":
        print "inserted key = " + str(message.key) + " value = " + str(message.value)
    else:
        print "updated key = " + str(message.key) + " value = " + str(message.value)
    currentCommand = None

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

    # create and start senders for this node
    senders = []
    central_sender = None
    for nodeName in nodes:
        # init responses to send queue
        responses_to_send[nodeName] = Queue.Queue()

        # if this is the central server, init a central sender
        if (nodeName == CENTRAL_SERVER_NAME):
            central_sender = CentralSender(max_delay, myNodeName, nodeName, *nodes[nodeName])
            central_sender.start()

        # else, build a normal sender
        elif (nodeName != myNodeName):
            sender = Sender(max_delay, myNodeName, nodeName, *nodes[nodeName])
            senders.append(sender)
            sender.start()

    print "=== Senders Initialized ==="

    # read commands from stdin until program is terminated
    while(1):
        message = raw_input()
        currentCommand = None
        message_data = message.split()

        ts = time.time()
        st = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')

        operation = str(message_data[0]).lower()

        if (operation == "send"):
            for sender in senders:
                if message_data[2] == sender.dest_name:
                    command = Message("send", None, None, None)
                    command.message = message_data[1]
                    sender.message_queue.put(command)

        elif (operation == "show-all"):
                keys = key_value_store.keys()
                for key in keys:
                    value = key_value_store[key]
                    print "key = " + str(key) + " value = " + str(value[0])

        elif (operation == "get"):
                message = Message(operation, message_data[1], None, message_data[2])
                currentCommand = (message.command, message.key, message.model)
                acksReceived = []

                # print "get command. currentCommand: " + str(currentCommand) #DEBUG

                # if linearizability, send to central server
                if message.model == 1:
                    central_sender.message_queue.put(message)

                    while len(acksReceived) < 1:
                        time.sleep(0.05)
                    print "get returned key = " + str(message.key) + " value = " + str(acksReceived[0])

                # if seq. consistency, can return local value
                elif message.model == 2:
                    value = key_value_store[message.key]
                    print "get returned key = " + str(message.key) + " value = " + str(value[0])

                # else, we need to perform operation and wait for acks
                elif message.model == 3 or message.model == 4:
                    # wait for acks from all the nodes in order to repair inconsistencies
                    numAcksNeeded = 4

                    # perform get
                    data = key_value_store[message.key]
                    acksReceived.append(data)

                    # send command to all neighbor nodes
                    for sender in senders:
                        if sender.dest_name != myNodeName:
                            sender.message_queue.put(message)

                    # wait to receive enough acks
                    while len(acksReceived) < numAcksNeeded:
                        time.sleep(0.01)

                    # once we have enough acks, sort the responses by timestamp and perform inconsistency repair
                    latestData = sorted(acksReceived, key=itemgetter(2))[0]
                    key_value_store[message.key] = latestData

                    # print out value and proceed to read in a new command
                    print "get: key = " + str(message.key) + " and value = " + latestData[0]
                    currentCommand = None

        elif (operation == "delete"):
            message = Message(operation, message_data[1], None, None)
            del key_value_store[message.key]
            # add delete message into every node's queue
            for sender in senders:
                if sender.dest_name != myNodeName:
                    sender.message_queue.put(message)
            # no need to wait for ACKs

        elif (operation == "search"):
            message = Message(operation, message_data[1], None, None)
            message.source = myNodeName
            currentCommand = (message.command, message.key)
            
            keys = key_value_store.keys()
            if message.key in keys:
                print myNodeName
            for sender in senders:
                if sender.dest_name != myNodeName:
                    sender.message_queue.put(message)

            while len(acksReceived) < 3:
                time.sleep(0.01)

        elif (operation == "delay"):
            delay_amount = int(message[1])
            time.sleep(delay_amount)

        else:
            # parse message generically
            message = Message(operation, message_data[1], message_data[2], message_data[3])
            currentCommand = (message.command, message.key, message.value, message.model)
            acksReceived = []

            if (operation == "insert"):
                message_keys = key_value_store.keys()

                # if this key already exists, don't overwrite it
                if message.key in message_keys:
                    print "The key you requested already exists. Use 'update' to change its value."

                # else, insert the new value
                else:
                    insertValue(message)

            elif (operation == "update"):
                message_keys = key_value_store.keys()

                # if this key already exists, update it
                if message.key in message_keys:
                    insertValue(message)

                # else, this is an error
                else:
                    print "The key you requested does not exist."
                  

