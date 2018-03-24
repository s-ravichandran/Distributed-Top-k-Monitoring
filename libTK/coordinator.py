import json, yaml
from libTK import *
import threading, socket
from libTK import comm
from libTK import settings
import time
import copy
import operator
import csv
import sys

class Coordinator():
    """
        Maintains the global top-k set
        Receives any new messages, and calculates if any updates are necessary

    """

    def __init__(self, k, epsilon, bandwidth, nodeport, outputname):
        """ Receives number of nodes.
            Looks up based on hostname to find all ip addresses
            Stores hostname 
            Contacts all nodes, gets all current object counts
        """
        # READ FILE to get hostnames, ips 
        self.ips = yaml.load(open(settings.FILE_SIMULATION_IPS, 'r'))

        self.nodes = self.ips['nodes']
        for hn, node in self.nodes.iteritems():
            node['testComplete'] = False 

        self.nodeport = nodeport
        self.F_coord = 0.5
        self.epsilon = epsilon
        self.k = k
        self.topk_iter = 0
        self.topk = []


        # VARIABLE EPSILON CHANGES
        self.targetBandwidth = float(bandwidth)
        # What percentage of bandwidth estimation comes from the new version
        self.alpha = 0.7
        self.estBandwidth = float(bandwidth)
        self.timeBetweenAdjusts = 10.0
        if (bandwidth == 0):
            self.useBandwidth = False
        else:
            self.useBandwidth = True
            # need to set so we can multiply/divide
            self.epsilon = 10.0
        self.bandwidth_list = []
        self.prev_band_time = 0.0
        self.epsilonAdjuster = threading.Thread(target=self.adjustEpsilon)


        self.running = True

        
        self.results_path = '%s/c0.csv' % outputname
        
        self.start_time = 0
        self.output_list = []


        #############################################################
        # LOCKS
        self.dataLock = threading.Lock()
        self.resolveLock = threading.Lock()
        self.outputLock = threading.Lock()
        self.bandwidthLock = threading.Lock()
        self.epsilonLock = threading.Lock()


        #############################################################
        # THREADS

        # Original thread needs to stay open to listen as server
        # Contacts all nodes, performs resolution, sets up initial parameters       
        performInit_thread = threading.Thread(target=self.sendStartCmd)
        performInit_thread.start()

        # we need a bunch of simultaneous threads for proper operation
        output_thread = threading.Thread(target=self.outputData)
        output_thread.start()
        
        # Wait for 10 seconds for enough data to be generated
        time.sleep(3)

        # Perform initial resolution
        initial_resolve_thread = threading.Thread(target=self.performInitialResolution)
        initial_resolve_thread.start()

    #########################################################################################################
    #########################################################################################################
    def adjustEpsilon(self):
        while (self.running):
            self.bandwidthLock.acquire()
            bandwidth_msgs = copy.deepcopy(self.bandwidth_list)
            self.bandwidth_list = []
            self.bandwidthLock.release()

            out.info("______________________________________\n")
            currtime = time.time()
            totalBytes = 0
            for msg in bandwidth_msgs:
                totalBytes += sys.getsizeof(json2str(msg))

            if (self.prev_band_time != 0.0):
                # TODO - react more quickly by factoring in difference between old est and new est
                out.info("oldEstBandwidth: %s.\n" % self.estBandwidth)
                diff_time = currtime - self.prev_band_time
                inst_band = totalBytes / diff_time
                out.info("instBandwidth: %s.\n" % inst_band)
                self.estBandwidth = ((1 - self.alpha) * self.estBandwidth) + (self.alpha * inst_band)
                out.info("newEstBandwidth: %s.\n" % self.estBandwidth)

                self.epsilonLock.acquire()
                old_epsilon = self.epsilon
                # Increase or decrease epsilon multiplicatively so we try to adjust bandwidth
                # First try: adjust by the percent difference from ideal
                if (self.estBandwidth > self.targetBandwidth):
                    # We need to cut down on bandwidth, target will be less than est
                    # So want to make epsilon bigger
                    if (self.epsilon <= 0.5):
                        self.epsilon = 0.5 * self.estBandwidth/self.targetBandwidth
                    else:
                        self.epsilon *= self.estBandwidth/self.targetBandwidth
                else:
                    # We can add bandwidth and make more accurate
                    # So want to make epsilon smaller
                    if (self.epsilon >= 0.5):                    
                        self.epsilon *=  self.estBandwidth/self.targetBandwidth
                    else:
                        # Go to 0 
                        self.epsilon = 0.0

                self.epsilonLock.release()      
                
                # If the total bytes are 0, perform a resolution to update the top-k so we can get more accurate results
                # TODO switch to check if old epsilon was also 0
                if (totalBytes == 0 and self.epsilon != old_epsilon):
                    self.resolveLock.acquire()
                    self.performReallocation(res=self.topk, host=None, topkObjects=self.topk) 
                    self.resolveLock.release()

                out.info("epsilon: %s.\n" % self.epsilon)
 
            self.prev_band_time = currtime 
            out.info("______________________________________\n")

            time.sleep(self.timeBetweenAdjusts)
        


    #########################################################################################################
    

    #########################################################################################################
    #########################################################################################################
    def outputData(self):
        while (self.running):
            self.outputLock.acquire()
            rowsOut = copy.deepcopy(self.output_list)
            self.output_list = []
            self.outputLock.release()

            formattedRows = []
            for r in rowsOut:
                currtime = r[0]
                send_rcv = r[1]
                msg = r[2]
                epsilon = r[3]
                hn = msg.get("hn", "None")
                msgType = msg.get("msgType", "None")
                size = sys.getsizeof(json2str(msg))
                formattedRows.append([currtime, send_rcv, hn, msgType, size, epsilon])


            #out.info("Outputting to file.\n")
            if len(formattedRows) > 0:
                ##########################################
                # SAVE THE INCOMING DATA TO A FILE        
                f = open(self.results_path, 'ab+')
                writer = csv.writer(f)
                writer.writerows(formattedRows)            
                f.close()
                ##########################################
            
            testComplete = True
            for hn, node in self.nodes.iteritems():
                if (not node['testComplete']):
                    testComplete = False
                    break

            if (testComplete):
                currtime = time.time()
                # We should have completed everything, so output a final row to the file and exit
                ##########################################
                #outwarn("Reached duration, exiting.\n")
                outrow = [currtime, 'STOPTEST', "None", "None", "None", "None"]
                f = open(self.results_path, 'ab+')
                writer = csv.writer(f)
                writer.writerow(outrow)            
                f.close()
                self.stop()
                return
                ##########################################
                
            time.sleep(5)
            
            

    #########################################################################################################

    #########################################################################################################
    #########################################################################################################
    def stop(self):
        self.running = False
    #########################################################################################################

    #########################################################################################################
    #########################################################################################################
    def addToOut(self, row):
        currtime = time.time
        self.outputLock.acquire()
        self.output_list.append(row)
        self.outputLock.release()
    #########################################################################################################
    
    #########################################################################################################
    #########################################################################################################
    def addToBand(self, msg):
        currtime = time.time
        self.bandwidthLock.acquire()
        self.bandwidth_list.append(msg)
        self.bandwidthLock.release()
    #########################################################################################################
    


    #########################################################################################################
    #########################################################################################################
    def send_msg(self, addr, msg):
        currtime = time.time()
        #out.warn("msg: %s\n" % msg)
        outrow = [currtime, 'send', msg, self.epsilon]
    
        self.addToOut(outrow)
        self.addToBand(msg)
        comm.send_msg(addr, msg)    
    #########################################################################################################


    #########################################################################################################
    #########################################################################################################
    def getSomePartials(self, ignore_host, resolution_set):
        """
            Send a message to each node asking to get partial values. 
            Should set waiting to True for each node so we can wait for the proper response
        """
        for hn, node in self.nodes.iteritems():
            if (hn != ignore_host): 
                node['waiting'] = True
                
                msg = {"msgType": settings.MSG_GET_SOME_OBJECT_COUNTS, "hn": hn, "data": resolution_set}
                self.send_msg((node['ip'], self.nodeport), msg)
    #########################################################################################################
    
    #########################################################################################################
    #########################################################################################################
    def resolve(self, hn, data):
        """
            Perform the entire resolution.
            Check if we all global constraints are still valid
                If so we simply reallocate among the coord/specific node
                If not we must contact all nodes, get all the values in the resolution set.

        """
        # Get a lock so only one resolution
        self.resolveLock.acquire()


        violated_objects = data['violations']
        topk = data['topk']
        topk_iter = data['topk_iter']
        
        out.info("Checking resolve for %s\nViolated: %s.\n" % (hn, violated_objects))

        # Set new stats so reallocation works properly        
        self.setObjectStats(hn, data)
        

        # Don't process any messages that refer to old data
        if (self.topk_iter > topk_iter):
            out.warn("Data for %s out of date, removing.\n" % hn)
            self.resolveLock.release()
            return

        resolution_set = violated_objects
        resolution_set.extend(topk)
        #out.info("Resolution set: %s.\n" % resolution_set)


        #out.info("checking if valid for host: %s.\n" % hn)
        stillValid = self.validationTest(hn, violated_objects, topk)
        # Check if topk is valid, if not don't resolve
       
        if stillValid:
            out.info("TOPK still valid, performing reallocation.\n")
            self.performReallocation(res=resolution_set, host=hn, topkObjects=topk) 
        else:
            out.warn("TOPK no longer valid, getting all partial objects.\n")
            self.getSomePartials(hn, resolution_set)

            # Blocking, will not complete until everything is completed
            self.waitForResponses()
            self.performReallocation(res=resolution_set, host=None, topkObjects=None) 


        out.info("NEW TOPK OBJECTS: %s\n" % self.topk)
        #out.info("node: %s\n" % self.nodes)
        #out.info("coord: %s\n" % self.coordVals)


        #self.verifyVals()
        self.resolveLock.release()
    #########################################################################################################


    #########################################################################################################
    #########################################################################################################
    def verifyVals(self):
        """ CHECKS TO SEE IF INVARIANTS ACTUALLY HOLD
        """
        objs = self.coordVals['partials'].keys()

        sums = {}

        for o in objs:
            sums[o] = self.coordVals['partials'][o]['param']

        for hn, node in self.nodes.iteritems():
            for o in objs:
                sums[o] += node['partials'][o]['param']

        print("SUMS: %s" % sums)

    #########################################################################################################
    




    #########################################################################################################
    #########################################################################################################
    def validationTest(self, hn, violated_objects, topk):
        """
        """
        partials_at_node = self.nodes[hn]['partials']
        for top_obj in topk:
            # a, b, c
            for obj in violated_objects:
                partial_val = partials_at_node[obj]
                partial_val_topk = partials_at_node[top_obj]

                if (obj not in self.coordVals['partials']):
                    self.coordVals['partials'][obj] = {'val': 0.0, 'param': 0.0}
                if (top_obj not in self.coordVals['partials']):
                    self.coordVals['partials'][top_obj] = {'val': 0.0, 'param': 0.0}

                coord_param = self.coordVals['partials'][obj]['param']
                coord_param_topk = self.coordVals['partials'][top_obj]['param']

                if (partial_val['val'] + partial_val['param'] + coord_param > partial_val_topk['val'] + partial_val_topk['param'] + coord_param_topk):
                    out.warn("Detected violation: %s violates top-k object: %s.\n" % (obj, top_obj))
                    # If any violated global totals are greater than any top k totals, we must do reallocation
                    return False

        # If we get here everything should be ok, so return True
        return True
    #########################################################################################################


    #########################################################################################################
    #########################################################################################################
    def receivedData(self, requestSock, msg):
        """ 
            Listens for any response messages. They are detailed below.
            Each response contains the name of the node for each lookup.


            getValsResponse:       

 
        """
        #out.warn("RECV_MSG: %s\n" % msg)

        msgType = msg['msgType']
        hn = msg['hn']

        currtime = time.time()
        outrow = [currtime, 'recv', msg, self.epsilon]
        self.addToOut(outrow)
        self.addToBand(msg)

        if   (msgType == settings.MSG_GET_OBJECT_COUNTS_RESPONSE):
            # From generator, request for a specific node should increment value by 1.
            self.setObjectStats(hn, msg['data'])
        elif (msgType == settings.MSG_CONST_VIOLATIONS):
            # Phase 2
            self.resolve(hn, msg['data'])
        elif (msgType == settings.MSG_TEST_COMPLETE):
            # Phase 2
            self.setHostComplete(hn)
    #########################################################################################################


    #########################################################################################################
    #########################################################################################################
    def sendStartCmd(self):
        self.start_time = time.time()
        outrow = [self.start_time, 'STARTTEST', {}, self.epsilon]
        self.addToOut(outrow)

        for hn, node in self.nodes.iteritems():
            msg = {"msgType": settings.MSG_START_GEN, "hn": hn}
            self.send_msg((node['ip'], self.nodeport), msg)
    #########################################################################################################

    
    #########################################################################################################
    #########################################################################################################
    def setBorderVal(self, res):
        # Compute Border Value B for this node
        # min adjusted value among topk items

        if (res == None):
            self.coordVals['border'] = 0.0
            return

        
        partials = self.coordVals['partials']

        # Max adjusted value among non top k items
        max_non_res = -100000
        for obj in partials.keys():
            if obj not in res:
                if(partials[obj]['param'] > max_non_res):
                    max_non_res = partials[obj]['param']

        out.info("setting border: %s.\n" % max_non_res)
        self.coordVals['border'] = max_non_res
    #########################################################################################################



    #########################################################################################################
    #########################################################################################################
    def performReallocation(self, res=None, host=None, topkObjects=None):
        """
        """

        try:

            if (topkObjects):
                setTopK = False
            else:
                setTopK = True

            participatingSum = {}
            borderSum = 0
            aggregateSum = {}
    
            if (host):
                hns = [host]
            else:
                hns = self.nodes.keys()

            #out.info("res: %s.\n" % res)
            #out.info("host: %s.\n" % host)
            #out.info("topkObjects: %s.\n" % topkObjects)
            #out.info("1\n")
            for hn in hns:
                node = self.nodes[hn]
            
                borderSum += node['border']
                for key, info in node['partials'].iteritems():
                    if (res is not None and key not in res):
                        #out.warn("REALLOC: skipping object %s.\n" % key)
                        continue
 
                    # We have already seen key, just add to this key
                    if key in participatingSum:
                        participatingSum[key] += info['val'] + info['param']
                        aggregateSum[key] += info['val']
                    else:
                        participatingSum[key] = info['val'] + info['param']
                        aggregateSum[key] = info['val']

            #out.info("2\n")
            ###########################################################################
            #out.info("3\n")
            
            
            self.setBorderVal(res)

            
            # Calculate correct border value at coordinator
            borderSum += self.coordVals['border']
            # Add coordinator adjustment factors to participatingSum
            for key, info in self.coordVals['partials'].iteritems():
                if (res is not None and key not in res):
                    #out.warn("REALLOC: skipping object %s.\n" % key)
                    continue

                # We have already seen key, just add to this key
                if key in participatingSum:
                    participatingSum[key] += info['val'] + info['param']
                    aggregateSum[key] += info['val']
                else:
                    participatingSum[key] = info['val'] + info['param']
                    aggregateSum[key] = info['val']


    
            #out.info("Participating sum: %s.\n" % participatingSum)
            #out.info("Aggregate sum: %s.\n" % aggregateSum)
            #out.info("Border sum: %s.\n" % borderSum)
            #out.err("Host h1: %s\n" % self.nodes['h1'])
            #out.err("coord: %s\n" % self.coordVals)

            ###################################################
            # SORT TO GET TOP K
            if (topkObjects is None):
                sortedVals = self.sortVals(aggregateSum)
                # In single case, particating is the resolution set and topk is already known
                res = [a[0] for a in sortedVals]
                topkObjects = [a[0] for a in sortedVals[0:self.k]]
                self.topk = topkObjects

            #out.info("res: %s.\n" % res)
            #out.info("topkObjects: %s.\n" % topkObjects)

            self.epsilonLock.acquire()
            ####################################################
            # CALCULATE LEEWAY
            leeway = {}
            
            for o in res:
                # If the object is in the top k set, we need to include epsilon
                leeway[o] = participatingSum[o] - borderSum
                if (o in topkObjects): 
                    leeway[o] += self.epsilon
    
    
            #out.info("leeway: %s.\n" % leeway)
    
            #####################################################
            # ASSIGN ADJUSTMENT FACTORS
            for hn in hns:
                node = self.nodes[hn]
                for o in res:
                    border = node.get('border', 0.0)
                    if (o not in node['partials']):
                        node['partials'][o] = {'val': 0.0, 'param': 0.0}
                    

                    partialVal = node['partials'][o]['val']

                    allocLeeway = node['F']*leeway[o]

                    node['partials'][o]['param'] = border - partialVal + allocLeeway
                #out.err("Host: %s\npartials: %s\nborder: %s\n" % (hn, node['partials'], border))
           
            #out.info("4\n")
            #####################################################
            # ASSIGN ADJUSTMENT FACTORS FOR COORDINATOR
            for o in res:
                border = self.coordVals.get('border', 0.0)
                if (o not in self.coordVals['partials']):
                    self.coordVals['partials'][o] = {'val': 0.0, 'param': 0.0}

                partialVal = self.coordVals['partials'][o]['val']
                    
                allocLeeway = self.coordVals['F']*leeway[o]
                self.coordVals['partials'][o]['param'] = border - partialVal + allocLeeway
                if (o in topkObjects):
                    self.coordVals['partials'][o]['param'] -= self.epsilon

            self.epsilonLock.release()

            #out.err("coord: %s\n" % self.coordVals)
              
            #out.info("5\n")
            ## Top k now determined, send message to each of the nodes with top k set and adjustment factors
            if (setTopK):    
                self.topk_iter += 1


            for hn in hns:
                node = self.nodes[hn]
                sendData = {}
                sendData['partials'] = node['partials']
            
                if (setTopK):     
                    sendData['topk'] = topkObjects
                    sendData['topk_iter'] = self.topk_iter
                    msg = {"msgType": settings.MSG_SET_TOPK, 'hn': hn, 'data': sendData}
                else: 
                    msg = {"msgType": settings.MSG_SET_NODE_PARAMETERS, 'hn': hn, 'data': sendData}
               
                self.send_msg((node['ip'], self.nodeport), msg)

        except Exception as e:
            out.err('calcEverything Exception: %s\n' % e)    
    #########################################################################################################

    #########################################################################################################
    #########################################################################################################
    def waitForResponses(self):
        """
            waits until all nodes have responded with their partial values.

        """
        waiting = False

        while (self.running):
            waiting = False
            # Iterate through nodes, make sure all have responded
            for hn, node in self.nodes.iteritems():
                if (node['waiting']):
                    #out.info("node: %s\n" % hn)
                    waiting = True

            # If we are no longer waiting on any nodes return
            if (not waiting):
                return

            # Sleep a little so we don't waste cycles        
            time.sleep(0.5)                
    #########################################################################################################

    #########################################################################################################
    #########################################################################################################
    def setObjectStats(self, hn, data):
        """
            Updates the initial values at node hn. 
        """
        # TODO - use a copy of nodes so we can handle a resolution set that isn't everything
        #out.info("Setting object stats for host: %s\n" % hn)
        self.dataLock.acquire()     

        self.nodes[hn]['border'] = data['border']
        self.nodes[hn]['waiting'] = False
        for obj, info in data['partials'].iteritems():
            self.nodes[hn]['partials'][obj] = info
        self.dataLock.release()

    #########################################################################################################
    
    #########################################################################################################
    #########################################################################################################
    def setHostComplete(self, hn):
        """
            Says test complete at the node. 
        """
        self.dataLock.acquire()     
        self.nodes[hn]['testComplete'] = True
        self.dataLock.release()

    #########################################################################################################

    #########################################################################################################
    #########################################################################################################
    def setTopK(sortVals):

        # Set the top k value, only keep the top however if not enough objs
        if (len(sortVals) < self.k):
            self.topk = sortVals
        else:
            self.topk = sortVals[0:self.k]

    #########################################################################################################

    #########################################################################################################
    #########################################################################################################
    def sortVals(self, vals):
        """ 
            Expects a dictionary of d[key] = value
            Returns a sorted array of (key, value) tuples
        """

        sortedVals = sorted(vals.items(), key=operator.itemgetter(1), reverse=True)
        return sortedVals
    #########################################################################################################
    



    #########################################################################################################
    #########################################################################################################
    def performInitialResolution(self):
            

        # TODO add resolution lock, only one resolution can occur at once
        # TODO separate these into functions, make it so initial top k query is the same as later queries

        # Start process to get initial top k values, this will be used to set thresholds at each node
        # Responses must be handled asynchronously
        #outinfo("Sending requests for all data to each node for initial top-k computation.\n")
        
        self.F_node = (1.0 - self.F_coord) / len(self.nodes) 

        
        for hn, node in self.nodes.iteritems():
            node['partials'] = {}
            node['border'] = 0
            node['F'] = self.F_node
            node['testComplete'] = False
            node['waiting'] = True
            
            msg = {"msgType": settings.MSG_GET_OBJECT_COUNTS, 'hn': hn}
            self.send_msg((node['ip'], self.nodeport), msg)

        self.coordVals = {}
        self.coordVals['partials'] = {}
        self.coordVals['border'] = 0.0
        self.coordVals['F'] = self.F_coord
       
 
        self.resolveLock.acquire()
        #outinfo("Waiting for all responses to arrive.\n")
        # Will wait until all nodes have values
        self.waitForResponses()        
        
        #outinfo("Responses arrived, performing reallocation.\n")
        # NEED TO CALCULATE TOP K SO WE CAN ASSIGN CORRECT BORDER VALUES
        self.fixInitBorderVals() 

        self.performReallocation()

        #outinfo("Initial reallocation complete.\n")
        self.resolveLock.release()

        # We can start tracking the bandwidth now if necessary
        if self.useBandwidth:
            self.epsilonAdjuster.start()
    #########################################################################################################



    #########################################################################################################
    #########################################################################################################
    def fixInitBorderVals(self):
        hns = self.nodes.keys()
        aggregateSum = {}

        for hn in hns:
            node = self.nodes[hn]
            for key, info in node['partials'].iteritems():
                if key in aggregateSum:
                    aggregateSum[key] += info['val']
                else:
                    aggregateSum[key] = info['val']

        # SORT TO GET TOP-K
        sortedVals = self.sortVals(aggregateSum)
        topkObjects = [a[0] for a in sortedVals[0:self.k]]
        res = [a[0] for a in sortedVals]
        for hn in hns:
            node = self.nodes[hn]
            # Compute Border Value B for this node
            # min adjusted value among topk items
            min_topk = 0.0  
            firstMin = True  
            partialCopy = node['partials']
            for obj in topkObjects:
                if(firstMin or ((partialCopy[obj]['val'] + partialCopy[obj]['param']) < min_topk)):
                    min_topk = (partialCopy[obj]['val']) + (partialCopy[obj]['param'])
                    firstMin = False

            firstMax = True
            # Max adjusted value among non top k items
            max_non_res = 0.0
            for obj in partialCopy.keys():
                if obj not in res:
                    if(firstMax or ((partialCopy[obj]['val'] + partialCopy[obj]['param']) > max_non_res)):
                        max_non_res = (partialCopy[obj]['val']) + (partialCopy[obj]['param'])
                        firstMax = False

            border_value = min(min_topk, max_non_res)
            node['border'] = border_value

    #########################################################################################################
    
