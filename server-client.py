# Server-client hybrid

import SocketServer
import socket
import sys
import threading

class MyTCPHandler(SocketServer.BaseRequestHandler):
    """
    The RequestHandler class for our server.

    It is instantiated once per connection to the server, and must
    override the handle() method to implement communication to the
    client.
    """

    def handle(self):
        # self.request is the TCP socket connected to the client
        self.data = self.request.recv(1024).strip()
        print "{} wrote:".format(self.client_address[0])
        print self.data
        # just send back the same data, but upper-cased
        self.request.sendall(self.data.upper())

def startServer(host, port):
    # Create the server, binding to localhost on port 9999
    server = SocketServer.TCPServer((host, port), MyTCPHandler)

    print "server started on port " + str(port)

    # Activate the server; this will keep running until you
    # interrupt the program with Ctrl-C
    server.serve_forever()

def startClient(host, port):
    # Create a socket (SOCK_STREAM means a TCP socket)
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    data = ""

    try:
        # Connect to server and send data
        sock.connect((host, port))
        print "client listening on port " + str(port)

        sock.sendall(data + "\n")

        # Receive data from the server and shut down
        received = sock.recv(1024)
    finally:
        sock.close()

    print "Sent:     {}".format(data)
    print "Received: {}".format(received)

if __name__ == "__main__":
    # start server thread for this node
    serverPort = int(sys.argv[1])
    serverThread = threading.Thread(name='server', target=startServer, args=("localhost", serverPort))
    serverThread.start()

    # wait for other 4 servers to be started
    input("Press Enter to launch clients...")

    # start client threads
    # clientThreads = []
    # for i in range(3):
    #     clientThread = threading.Thread(target=startClient, args=("localhost", 5001 + i))
    #     clientThreads.append(clientThread)
    #     clientThread.start()

    clientPort = int(sys.argv[1]) - int(sys.argv[1])%2
    clientThread = threading.Thread(target=startClient, args=("localhost", clientPort))
    clientThread.start()


    

