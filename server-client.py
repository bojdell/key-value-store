# Server-client hybrid

import SocketServer
import socket
import sys
import threading
import Queue
import time
import datetime
import random

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
            if received:
                ts = time.time()
                st = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
                data = received.split()
                print "Received \"" + data[1] + "\" from " + data[0] + " max delay is " + str(self.max_delay) + " s, system time is " + st


class Sender():
    """
    Class to open a connection to another node's Listener and send messages
    """

    def __init__(self, max_delay, name, host, port):
        self.name = name
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
            if self.message_queue.empty():
                time.sleep(0.01)
            else:
                message_data = self.message_queue.get()
                delay = random.random() * self.max_delay
                ts = time.time()
                st = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
                print "Sent \"" + message + "\" to " + self.name + ", system time is " + st
                time.sleep(delay)
                sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                sock.sendto(self.name + " " + message, (self.host, self.port))

# usage: server-client.py conf.txt A input.txt
if __name__ == "__main__":
    myNodeName = sys.argv[2]
    config_file = open(sys.argv[1],'r')
    #input_file = open(sys.argv[3], 'r')
    delay_info = config_file.readline()
    max_delay = int(delay_info)
    nodes = {}

    for line in config_file:
        node_info = line.split()
        nodes[node_info[2]] = (node_info[0], int(node_info[1]))

    socket.setdefaulttimeout(None)

    # create and start Listener for this node
    listener = Listener(max_delay,*nodes[myNodeName])
    listener.start()
    print "=== Listener Initialized ==="

    # wait for other nodes to be started
    time.sleep(0.05)
    raw_input("Press Enter to launch senders...")

    # create and start senders for this node
    senders = []
    for nodeName in nodes:
        if(nodeName != myNodeName):
            sender = Sender(max_delay, nodeName, *nodes[nodeName])
            senders.append(sender)
            sender.start()

    print "=== Senders Initialized ==="

    #raw_input("Press Enter to begin sending messages...")

    # start by reading messages in the input file
    """for line in input_file:
        message_data = line.split()
        if (message_data[0] == "send"):
            for sender in senders:
                if message_data[2] == sender.name:
                    sender.message_queue.put((message_data[1], message_data[2]))
        time.sleep(0.05)"""

    # read commands from stdin until program is terminated
    while(1):
        message = raw_input()
        message_data = message.split()
        if (message_data[0] == "send"):
            for sender in senders:
                if message_data[2] == sender.name:
                    sender.message_queue.put((message_data[1], message_data[2]))
        


