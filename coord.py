import sys, json, argparse, copy, signal, time, os
sys.path.append('/home/mininet/simulator/')

import socket, threading, SocketServer
from libTK import *
from libTK import settings
from libTK import coordinator
from libTK import comm



class ThreadedTCPRequestHandler(SocketServer.BaseRequestHandler):

    def handle(self):
        # Receive the data, parse into json object
        data = str2json(self.request.recv(4096))

        # Pass off to coord object so it can check for violations
        self.server.coord.receivedData(self.request, data) 

        
class ThreadedTCPServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer):


    def __init__(self, server_address, handler_class, coord):
        """
        Keep track of coord object which will handle all calculations
        """
        self.allow_reuse_address = True
        self.coord = coord
        SocketServer.TCPServer.__init__(self, server_address, handler_class)


def setupArgParse():
    p = argparse.ArgumentParser(description='Daemon for ParaDrop Framework Control Configuration server')
    p.add_argument('-p', '--masterport', help='Port to listen on', type=int, default=11000)
    p.add_argument('-i', '--masterip', help='Host to listen on', type=str, default='localhost')
    p.add_argument('-k', '--topk', help='K objects to get', type=int, default=1)
    p.add_argument('-e', '--epsilon', help='Epsilon', type=int, default=0)
    p.add_argument('-d', '--nodeport', help='Port to send to nodes', type=int, default=10000)
    p.add_argument('-o', '--outputname', help='Name of the directory to output to', type=str, default='same')
    p.add_argument('-b', '--bandwidth', help='Target bandwidth', type=int, default=0)
    return p


if (__name__ == "__main__"):

    p = setupArgParse()
    args = p.parse_args()
    
    out.info('-- %s Starting master node: port: %s ipaddr: %s\n' % (logPrefix(), args.masterport, args.masterip))
    # Port 0 means to select an arbitrary unused port

    # Get current top-k queries
    coord = coordinator.Coordinator(args.topk, args.epsilon, args.bandwidth, args.nodeport, args.outputname)
    
    server = ThreadedTCPServer((args.masterip, args.masterport), ThreadedTCPRequestHandler, coord)
    ip, port = server.server_address

    # Start a thread with the server -- that thread will then start one
    server_thread = threading.Thread(target=server.serve_forever)
    server_thread.start() 


    # Start client if necessary

    # Listen for kill signal, shutdown everything
    try:
        while (coord.running):
            pass 
	else:
	    coord.stop()
	    server.shutdown()
    except KeyboardInterrupt:
	coord.stop()	
        server.shutdown()
    
