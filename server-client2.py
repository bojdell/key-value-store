# Server-client hybrid

import SocketServer
import socket
import sys
import threading
import Queue
import time

class MyTCPHandler(SocketServer.BaseRequestHandler):
    """
    The RequestHandler class for our Listeners

    It is instantiated once per connection to the server, and must
    override the handle() method to implement communication to the
    Sender.
    """

    def handle(self):
        while(1):
            self.data = self.request.recv(1024).strip()
            print "{} wrote:".format(self.client_address[0])
            print self.data

class Listener():
    """
    Class to listen for all incoming messages to this node
    """

    def __init__(self, host, port):
        self.host = host
        self.port = port

    def start(self):
        listenerThread = threading.Thread(target=self.__listen)
        listenerThread.setDaemon(True)
        listenerThread.start()

    def __listen(self):
        # Create a listener that will receive all incoming messages to this node
        listenSocket = SocketServer.TCPServer((self.host, self.port), MyTCPHandler)
        print "listening on port " + str(self.port)

        # Begin listening; this will keep running until interrupted by Ctrl-C
        listenSocket.serve_forever()

class Sender():
    """
    Class to open a connection to another node's Listener and send messages
    """

    def __init__(self, name, host, port):
        self.name = name
        self.host = host
        self.port = port
        self.message_queue = Queue.Queue()

    def start(self):
        senderThread = threading.Thread(target=self.__send)
        senderThread.setDaemon(True)
        senderThread.start()

    def __send(self):
        sendSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sendSocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sendSocket.connect((self.host, self.port))
        time.sleep(0.01)
        print "server ready to send on port " + str(self.port)

        while (1):
            if self.message_queue.empty():
                time.sleep(0.01)
            else:
                message_data = self.message_queue.get()
                print self.name + " sending: " + message_data[0] + " to node: " + message_data[1]
                sendSocket.sendall(message_data[0])
                sendSocket

# usage: server-client.py conf.txt A
if __name__ == "__main__":
    myNodeName = sys.argv[2]
    config_file = open(sys.argv[1],'r')
    delay_info = config_file.readline()
    max_delay = int(delay_info[0])
    nodes = {}

    for line in config_file:
        node_info = line.split()
        nodes[node_info[2]] = (node_info[0], int(node_info[1]))

    # create and start Listener for this node
    listener = Listener(*nodes[myNodeName])
    listener.start()
    print "=== Listener Initialized ==="

    # wait for other nodes to be started
    time.sleep(0.05)
    raw_input("Press Enter to launch senders...")

    # create and start senders for this node
    senders = []
    for nodeName in nodes:
        if(nodeName != myNodeName):
            sender = Sender("Sender " + myNodeName + " -> " + nodeName, *nodes[nodeName])
            senders.append(sender)
            sender.start()

    print "=== Senders Initialized ==="

    # read commands from stdin until program is terminated
    while(1):
        message = raw_input()
        message_data = message.split()
        for sender in senders:
            sender.message_queue.put((message_data[1], message_data[2]))

