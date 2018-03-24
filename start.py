#!/usr/bin/python

import json, argparse, sys
sys.path.append('/home/mininet/simulator/')

from mininet.topo import Topo
from mininet.net import Mininet
from mininet.util import dumpNodeConnections
from mininet.log import setLogLevel
from mininet.cli import CLI
from libTK import settings
import os


class SingleSwitchTopo(Topo):
    "Single switch connected to n hosts."
    def build(self, n=2):
        mainSwitch = self.addSwitch('s0')
        coord = self.addHost('c0')
        self.addLink(coord, mainSwitch)

        # Python's range(N) generates 0..N-1
        for i in range(n):
            host = self.addHost('h%s' % (i + 1))

            self.addLink(host, mainSwitch)

def saveNodesToFile(net, numnodes):
    cstr = 'c0'
    c = net.get(cstr)
    ips = {}
    ips['coords'] = {}
    ips['coords'][cstr] = {}
    ips['coords'][cstr]['ip'] = c.IP() 
    
    ips['nodes'] = {}
    # GET ALL ips of hosts for coordinator to read
    for i in range(numnodes):
        h = 'h%s' % (i + 1)
        ips['nodes'][h] = {}
        ips['nodes'][h]['ip'] = net.get(h).IP() 
    
    print ips    
    # Save all ips to file, so each program can access them 
    json.dump(ips, open(settings.FILE_SIMULATION_IPS, 'w'))
    return ips



def startScreens(net, numnodes, ips, nodeport, masterport, topk, epsilon, bandwidth, testname, outputname):
    # Start the screens on each machine
    for i in range(numnodes):

        # START THE NODES
        hn = 'h%s' % (i + 1)
        h = net.get(hn) 
        runCmd = 'screen -h 2000 -dmS %s python /home/mininet/simulator/mon.py --hostname %s --nodeport %s --nodeip %s --masterip %s --masterport %s --testname %s --outputname %s' % (hn, hn, nodeport, ips['nodes'][hn]['ip'], ips['coords']['c0']['ip'], masterport, testname, outputname)
        print(runCmd)
        h.cmd(runCmd)

    cstr = 'c0'
    c = net.get(cstr)
    # Start the coordinator machine
    runCmd = 'screen -h 2000 -dmS controller python /home/mininet/simulator/coord.py --masterport %s --masterip %s --topk %s --epsilon %s --bandwidth %s --nodeport %s --outputname %s' % (masterport, ips['coords']['c0']['ip'], topk, epsilon, bandwidth, nodeport, outputname)
    print(runCmd)
    c.cmd(runCmd)

def stopScreens(net, numnodes):
    # Start the screens on each machine
    killCmd = 'pkill -15 screen'

    for i in range(numnodes):
        hn = 'h%s' % (i + 1)
        h = net.get(hn) 
        h.cmd(killCmd)

    cstr = 'c0'
    c = net.get(cstr)
    # Start the coordinator machine
    c.cmd(killCmd)

def simpleTest(topk, epsilon, bandwidth, nodes, nodeport, masterport, testname, outputname):
    "Create and test a simple network"
    topo = SingleSwitchTopo(nodes)
    net = Mininet(topo)
    net.start()
  
    if (bandwidth == 0): 
        outputloc = 'results/%s/ep_%s' % (outputname, epsilon)
        if not os.path.exists(outputloc):
            os.makedirs(outputloc)
    else:
        outputloc = 'results/%s/band_%s' % (outputname, bandwidth)
        if not os.path.exists(outputloc):
            os.makedirs(outputloc)
 
    ips = saveNodesToFile(net, nodes)

    startScreens(net, nodes, ips, nodeport, masterport, topk, epsilon, bandwidth, testname, outputloc)
 
    CLI(net)

    stopScreens(net, nodes)

    net.stop()

def setupArgParse():
    p = argparse.ArgumentParser(description='Daemon for ParaDrop Framework Control Configuration server')
    p.add_argument('-k', '--topk', help='Top k objects', type=int, default=1)
    p.add_argument('-e', '--epsilon', help='Epsilon to consider', type=int, default=0)
    p.add_argument('-n', '--nodes', help='Number of nodes', type=int, default=8)
    p.add_argument('-s', '--nodeport', help='Host port to listen on', type=int, default=10000)
    p.add_argument('-p', '--masterport', help='Port of the master node.', type=int, default=11000)
    p.add_argument('-t', '--testname', help='Test data to select from.', type=str, default='same')
    p.add_argument('-o', '--outputname', help='Location to output test data to.', type=str, default='test')
    p.add_argument('-b', '--bandwidth', help='Target bandwidth (bytes/second) to use total.', type=int, default=0)
    return p

if __name__ == '__main__':

    p = setupArgParse()
    args = p.parse_args()

    # Tell mininet to print useful information
    setLogLevel('info')
    simpleTest(args.topk, args.epsilon, args.bandwidth, args.nodes, args.nodeport, args.masterport, args.testname, args.outputname)
