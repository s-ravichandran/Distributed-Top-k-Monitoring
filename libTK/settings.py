##################################################################
# Copyright 2013-2014 All Rights Reserved
# Authors: The Paradrop Team
###################################################################

"""
    This file contains any settings required by ANY and ALL modules of the paradrop system.
"""
import os, re, sys

#messages 
MSG_REQUEST_DATA = 'request'
MSG_SET_NODE_PARAMETERS = 'setNodeParams'
MSG_SET_TOPK = 'setTopK'
MSG_GET_OBJECT_COUNTS = 'getCounts'
MSG_GET_SOME_OBJECT_COUNTS = 'getSomeCounts'
MSG_GET_OBJECT_COUNTS_RESPONSE = 'getCountsResponse'
MSG_START_GEN = 'startGen'
MSG_STOP_GEN = 'stopGen'
MSG_TEST_COMPLETE = 'testComplete'
MSG_CONST_VIOLATIONS = 'handleViolations'


MON_ROLLING_WINDOW_TIME = 10

# Addresses / Port
RECV_PORT = 11000


# File locations
FILE_SIMULATION_IPS = '/home/mininet/simulator/ips.txt'


###############################################################################
# Helper functions
###############################################################################

def parseValue(key):
    """
        Description:
            Attempts to parse the key value, so if the string is 'False' it will parse a boolean false.

        Arguments:
            @key : the string key to parse

        Returns:
            The parsed value. If no parsing options are available it just returns the same string.
    """
    # Is it a boolean?
    if(key == 'True'):
        return True
    if(key == 'False'):
        return False
    
    # Is it None?
    if(key == 'None'):
        return None

    # Is it a float?
    if('.' in key):
        try:
            f = float(key)
            return f
        except:
            pass
    
    # Is it an int?
    try:
        i = int(key)
        return i
    except:
        pass

    # Otherwise, its just a string:
    return key

def updateSettingsList(slist):
    """
        Description:
            Take the list of key:value pairs, and replace any setting defined.
        
        Arguments:
            @slist: The key:value pairs

        Returns:
            None

        Throws:
            PDError
    """
    # Get a handle to our settings defined above
    settingsModule = sys.modules[__name__]
    for kv in slist:
        k,v = kv.split(':')
        # We can either replace an existing setting, or set a new value, we don't care
        setattr(settingsModule, k, parseValue(v))
