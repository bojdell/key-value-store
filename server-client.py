# Server-client hybrid

import SocketServer
import socket
import sys
import threading
import Queue

message_queue = Queue.Queue()

class MyTCPHandler(SocketServer.BaseRequestHandler):
    """
    The RequestHandler class for our server.

    It is instantiated once per connection to the server, and must
    override the handle() method to implement communication to the
    client.
    """

    def handle(self):
        while (1):
            if message_queue.empty():
                pass
            else:
                message_data = message_queue.get()
                print "Sending: " + message_data[0]
                self.request.sendall(message_data[0])

def startServer(host, port):
    # Create the server, binding to localhost on port 9999
    server = SocketServer.TCPServer((host, port), MyTCPHandler)
    print "server started on port " + str(port) + "\n"

    # Activate the server; this will keep running until you
    # interrupt the program with Ctrl-C
    server.serve_forever()

def startClient(host, port):
    # Create a socket (SOCK_STREAM means a TCP socket)
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.connect((host, port))
    print "client listening on port " + str(port) + "\n"
    while(1):
        # Receive data from the server
        received = sock.recv(1024)
        print "Received: {}".format(received)


if __name__ == "__main__":
    # start server thread for this node
    serverPort = int(sys.argv[1])
    serverThread = threading.Thread(name='server', target=startServer, args=("localhost", serverPort))
    serverThread.setDaemon(True)
    serverThread.start()

    # wait for other servers to be started
    raw_input("Press Enter to launch clients...")

    # start client threads
    # clientThreads = []
    # for i in range(3):
    #     clientThread = threading.Thread(target=startClient, args=("localhost", 5001 + i))
    #     clientThreads.append(clientThread)
    #     clientThread.start()

    if(serverPort == 5000):
        clientPort = 5001
    else:
        clientPort = 5000
    clientThread = threading.Thread(target=startClient, args=("localhost", clientPort))
    clientThread.setDaemon(True)
    clientThread.start()

    while(1):
        data = raw_input()
        message_data = data.split()
        message_queue.put((message_data[1],int(message_data[2])))

