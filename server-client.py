# Server-client hybrid

import SocketServer
import socket
import sys
import threading
import Queue

message_queue_A = Queue.Queue()
message_queue_B = Queue.Queue()
message_queue_C = Queue.Queue()
message_queue_D = Queue.Queue()

"""class MyTCPHandler(SocketServer.BaseRequestHandler):
    The RequestHandler class for our server.

    It is instantiated once per connection to the server, and must
    override the handle() method to implement communication to the
    client.

    def handle(self):
        while (1):
            if message_queue.empty():
                pass
            else:
                message_data = message_queue.get()
                print "Sending: " + message_data[0]
                self.request.sendall(message_data[0])"""

def startSender(nodeName, host, port):
    # Create the server, binding to localhost on port 9999
    #server = SocketServer.TCPServer((host, port), MyTCPHandler)
    #print "sender started on port " + str(port) + "\n"
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.settimeout(None)
    sock.connect((host,port))
    while 1:
        if nodeName == 'A':
            if not message_queue_A.empty():
                messageA = message_queue_A.get()
                print "Sending: " + messageA
                sock.sendall(messageA)
        if nodeName == 'B':
            if not message_queue_B.empty():
                messageB = message_queue_B.get()
                print"Sending: " + messageB
                sock.sendall(messageB)
        if nodeName == 'C':
            if not message_queue_C.empty():
                messageC = message_queue_C.get()
                print "Sending: " + messageC
                sock.sendall(messageC)
        if nodeName == 'D':
            if not message_queue_D.empty():
                messageD = message_queue_D.get()
                print "Sending: " + messageD
                sock.sendall(messageD)


    # Activate the server; this will keep running until you
    # interrupt the program with Ctrl-C

def startListener(serverName, host, port):
    # Create a socket (SOCK_STREAM means a TCP socket)
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.settimeout(None)
    sock.bind((host, port))
    sock.listen(1)
    conn,addr =sock.accept()
    #print serverName + "listening on port " + str(port) + "\n"
    while(1):
        # Receive data from the server
        received = conn.recv(1024)
        if received:
            print "Received: " + received


if __name__ == "__main__":
    # start server thread for this node
    myNodeName = sys.argv[2]
    config_file = open(sys.argv[1],'r')
    delay_info = config_file.readline()
    max_delay = int(delay_info[0])
    servers = {}

    for line in config_file:
        node_info = line.split()
        servers[node_info[2]] = (node_info[2], node_info[0], int(node_info[1]))

    listenerThread = threading.Thread(target=startListener, args=(servers[myNodeName]))
    listenerThread.setDaemon(True)
    listenerThread.start()

    raw_input("Press Enter to launch clients...")

    for nodeName in servers:
        if(nodeName != myNodeName):
            senderThread = threading.Thread(name='server', target=startSender, args=(servers[nodeName]))
            senderThread.setDaemon(True)
            senderThread.start()

    while(1):
        data = raw_input( )
        message_data = data.split()
        if(message_data[2] == "A"):
            message_queue_A.put(message_data[1])
        elif(message_data[2] == "B"):
            message_queue_B.put(message_data[1])
        elif(message_data[2] == "C"):
            message_queue_C.put(message_data[1])
        elif(message_data[2] == "D"):
            message_queue_D.put(message_data[1])

