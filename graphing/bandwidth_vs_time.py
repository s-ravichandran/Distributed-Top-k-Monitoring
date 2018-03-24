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



def toInt(tup):
    key, val = tup
    return int(key.split('_')[-1]), val

def plotSingleTest(ax_band, ax_ep, legend_str, data, scalarMap, color_index, running_avg_secs):
    times = []
    totalCost = []
    epsilons = []
    windowBytes = 0.0
    starttime = 0.0
    window = deque()

    print("plotting %s" % legend_str)

   
    starttime = float(data[0][0])
    stoptime = float(data[-1][0])
    runtime = stoptime - starttime

    i = 1
    curr_epsilon = 0
    currtime = 0.5
    while (currtime <= runtime):
        # add all data values which are greater than next step 
        while i<len(data):
            (time, send_rcv, host, msgType, numBytes, epsilon) = data[i]
            curr_epsilon = float(epsilon)
            nexttime = float(time)
            if (send_rcv == 'STARTTEST' or send_rcv == 'STOPTEST' or send_rcv == 'startGen' or msgType == 'testComplete'):
                i += 1
                continue

            nexttime = nexttime - starttime
            if (nexttime < currtime):
                numBytes = int(numBytes)         
                i += 1
                # Append this time, bytes to deque 
                window.append((nexttime, numBytes))
                windowBytes += numBytes
            else:
                break
        

        # Remove any old times from total bytes
        while (len(window) > 0 and (currtime - window[0][0] > running_avg_secs)):
            t, b = window.popleft()
            windowBytes -= b
        
        # Divide band by avg_time to get average bandwidth
        curr_band = windowBytes/running_avg_secs

        times.append(currtime)
        totalCost.append(curr_band) 
        epsilons.append(curr_epsilon)
 
        currtime += 1.0

        if (currtime + 20.0 > runtime):
            break  

    colorVal = scalarMap.to_rgba(color_index)
    ax_band.plot(times, totalCost, '.-', color=colorVal, label=legend_str)
    ax_ep.plot(times, epsilons, '.-', color=colorVal, label=legend_str)
   

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
 

def plotDist(ax_dist, dist):
    data, durations, scale = dist
    times, avgs = calcGlobalVals(data, durations)

    fixed_times = [scale*t for t in times]
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
        ax_dist.plot(fixed_times, avgs[i], '.-', color=colors[i], label=captions[i])






def graph(epsilon_data, band_data, running_average_time, dist):

    f, (ax_band, ax_ep, ax_dist) = plt.subplots(3, sharex=True)    
    
    ########################################################################
    # Graph the varying bandwidths

    sorted_epsilon = iter(sorted(epsilon_data.items(), key=toInt))
    index = 0
    
    num_eps = len(epsilon_data)
    winter = plt.get_cmap('winter')
    cNorm = colors.Normalize(vmin=0, vmax=num_eps)
    scalarMap_epsilon = cmx.ScalarMappable(norm=cNorm, cmap=winter)
    
    for ep, data in sorted_epsilon:
        legend_str = 'ep=%s' % int(ep)
        plotSingleTest(ax_band, ax_ep, legend_str, data, scalarMap_epsilon, index, running_average_time)
        index += 1

    ########################################################################
    # Graph the varying bandwidths
    sorted_band = iter(sorted(band_data.items(), key=toInt))
    index = 0
    
    num_bands = len(band_data)
    autumn = plt.get_cmap('autumn')
    cNorm = colors.Normalize(vmin=0, vmax=num_eps)
    scalarMap_band = cmx.ScalarMappable(norm=cNorm, cmap=autumn)
    
    for band, data in sorted_band:
        legend_str = 'band=%s' % int(band)
        plotSingleTest(ax_band, ax_ep, legend_str, data, scalarMap_band, index, running_average_time)
        index += 1

    ########################################################################
    # Graph the distributions
    plotDist(ax_dist, dist)

    ax_band.set_title('Average Bandwidth vs Time')
    ax_band.legend(loc=4)
    ax_band.set_ylabel("Bandwidth (bytes/s)")

    ax_ep.set_ylabel("Epsilon")
    ax_ep.legend(loc=4)
    ax_ep.set_title('Epsilon vs Time')

    ax_dist.set_title('Distribution vs Time')
    ax_dist.legend(loc=4)
    ax_dist.set_xlabel("Time (s)") 
    ax_dist.set_ylabel("Global object value") 

    plt.show()
    #plt.savefig('../report/images/train/err_comps/%s.png' % tests[i], dpi=300)
    #plt.close()


def setupArgParse():
    p = argparse.ArgumentParser(description='Graphing Bandwidth vs time for various epsilons/bandwidths')
    p.add_argument('-t', '--test_dir', help='Directory of test to graph', type=str, default="none")
    p.add_argument('-a', '--average', help='Running Average to Use', type=int, default=5)
    p.add_argument('-s', '--scale', help='Scaling factor to fix the times being too short for dist graph.', type=float, default=1.06)
    return p

if __name__ == '__main__':
    import glob
    
    p = setupArgParse()
    args = p.parse_args()

    # Dictionary indexed by epsilon
    epsilon_data = {}
    
    # Dictionary indexed by bandwidth
    band_data = {}

    for path in glob.glob('%s/ep_*' % args.test_dir):
        epsilon = path.split('_')[-1]
        epsilon_data[epsilon] = []
        with open('%s/c0.csv' % path, 'r') as f:
            reader = csv.reader(f)
            for row in reader:
                epsilon_data[epsilon].append(row)

    for path in glob.glob('%s/band_*' % args.test_dir):
        band = path.split('_')[-1]
        band_data[band] = []
        with open('%s/c0.csv' % path, 'r') as f:
            reader = csv.reader(f)
            for row in reader:
                band_data[band].append(row)


    whichTest = args.test_dir.split('/')[-1]

    # Load the data for the distributions
    testSpec = json.load(open('genData/%s.txt' % whichTest, 'r'))
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


    graph(epsilon_data, band_data, args.average, (data, durations, args.scale))      
