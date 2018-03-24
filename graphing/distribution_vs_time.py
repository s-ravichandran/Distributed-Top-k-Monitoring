import matplotlib.pyplot as plt
import matplotlib as m
import matplotlib.cm as cmx
import matplotlib.colors as colors
from numpy.random import rand
import numpy as np
import argparse
import json
import csv
from collections import deque


def calcGlobalVals(data, durations):
    """
        For each hostname, g
    """
    firstTime = True
    times = []
    totals = [[] for i in range(25)]
    initVals = [0 for i in range(25)]
    window_size = 10
    for hn, dist in data.iteritems():
        time = 0
        dur_index = 0
        print("hn: %s" % hn)
        #print("dist: %s" % dist)
        duration = durations[hn]
        num_durations = len(duration)
        running_total = 0
        running_vals = [deque() for i in range(25)]
        # For each hostname, calculate running average, add to total at each data point



        dur_countdown = duration[0]
        while (dur_index < num_durations):
            # For each key, add to total
            for i in range(25):
          

                node_val = dist[dur_index][i] 
                #print("dur_countdown: %s" % dur_countdown) 
                #print("dur_index: %s" % dur_index) 
                # Append the (time, dataval) point to the deque for the key
                running_vals[i].append((time, node_val))


                # Loop through the tail of the deque, remove any values that are older than 10 seconds 
                # Remove any old times from total bytes
                while (len(running_vals[i]) > 0 and (time - running_vals[i][0][0] > window_size)):
                    t, b = running_vals[i].popleft()

                total = 0
                for indv_time in running_vals[i]:
                    total += indv_time[1]

                # Add to total for that key
                if firstTime:
                    totals[i].append(total)
                else:
                    totals[i][time] += total
           
             
            if firstTime:    
                times.append(time)
            
            time += 1
            dur_countdown = dur_countdown - 1
    
            if (dur_countdown == 0):
                dur_index += 1            
                if (dur_index == num_durations):
                    break
                else:
                    dur_countdown = duration[dur_index]
                
        firstTime = False

    return times, totals

def graph(data, durations):

    times, avgs = calcGlobalVals(data, durations)
 
    graph_indices = [0, 9, 10, 12]
    colors = {0: 'green', 9:'blue', 10:'red', 12:'black'}
    captions = {0: 'a-i, in top-k',
                9: 'j',
                10: 'k',
                12: 'l-y, not in top-k'
                }

    for i in graph_indices:
        letter = chr(i + ord('a'))
        print("graphing %s" % letter)
        plt.plot(times, avgs[i], '.-', color=colors[i], label=captions[i])



    plt.title('Distributions')
    plt.legend(loc=2)
    
    plt.xlabel("Time") 
    plt.ylabel("Global Value")
    plt.show()
    #plt.savefig('../report/images/train/err_comps/%s.png' % tests[i], dpi=300)
    #plt.close()


def setupArgParse():
    p = argparse.ArgumentParser(description='Graphing Bandwidth vs time for various epsilons/bandwidths')
    p.add_argument('-g', '--gendata', help='The file to output', type=str, default="none")
    return p

if __name__ == '__main__':
    import glob, json
    
    p = setupArgParse()
    args = p.parse_args()


    # Load the data
    testSpec = json.load(open(args.gendata, 'r'))
    nodeDistribution = {}
    durations = {}
    data = {}
    for i in range(1, 11):
        hn = 'h%s' % i
        nodeDistribution[hn] = testSpec[hn]

        data[hn] = [[0 for col in range(25)] for row in range(len(nodeDistribution[hn]))]
        # Assume 10 seconds if not mentioned
        durations[hn] = [10 for row in range(len(nodeDistribution[hn]))]
        for i, d in enumerate(nodeDistribution[hn]):
            for key, val in d['freqs'].iteritems():
                data[hn][i][ord(key)-ord('a')] = val
            durations[hn][i] = d.get('duration', 10)

    graph(data, durations)      
