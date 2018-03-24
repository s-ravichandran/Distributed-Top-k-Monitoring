Distributed Global Top-K Simulator
============================
This is an adapted version of the Distributed Top-K Monitor Application as seen [from this 2003 Stanford Paper by Babcock/Olston](http://infolab.stanford.edu/~olston/publications/topk.html).

TEAM MEMBERS

- Srinivasan Ravichandran
- Michael Bauer

A technical report can be found [here](https://github.com/s-ravichandran/Distributed-Top-k-Monitoring/blob/master/740_topK.pdf)

INSTALL
------
    -need a mininet VM (use ubuntu)
    -sudo apt-get install screen
    -sudo apt-get install python-yaml
    -ssh into VM with x forwarding into chute to open up xterm when running experiments


RUN
------
Should run in a mininet VM - checkout repository and proceed to root directory

To run the variable oscillating example from the paper, run 

```
sudo python start.py -n 10 -t all_osc1_osc2 -k 10 -o all_osc1_osc2 -e 0
```

This will create a topology with one coordinator and 10 nodes.
It will read from genData/all\_osc1\_osc2.txt and output results to the results/all\_osc1\_osc2 directory (if data already exists you should remove it beforehand, or you will have two datasets in the same file)!
Currently this will run the algorithm with a fixed epsilon, as seen in the Babcock/Olston paper.
To run with our novel variable epsilon, run 

```
sudo python start.py -n 10 -t all_osc1_osc2 -k 10 -o all_osc1_osc2 -b 6000
```

This will create the same topology/dataset as before but run the version with variable epsilon/constant bandwidth.

 
VIEWING
--------------
To view the experiment as it is proceeding, open up xterm in the experiment terminal, and go to the appropriate screen:

```xterm c0```,
```
screen -r controller
```

or 

```xterm h1```,
```
screen -r h1
```

Additional printouts can be enabled by adding extra ```out.info```, ```out.warn```, and ```out.err``` commands in the code.

CODE STRUCTURE
-----------------
### start.py ###
* Takes in inputs
* Sets up topology
* Starts coordinator/monitor

### mon.py ###
* Runs a server on each monitor node
* Receives data from coordinator
* Passes this data to ```libTK/monitor.py```, where all logic is actually happening

### coord.py ###
* Runs a server on the coordinator node
* Receives data from monitors
* Passes this data to ```libTK/coordinator.py```, where all logic is actually happening


### graphing/bandwidth\_vs\_time.py ###
* Reads in results from some results folder
* uses matplotlib to plot bandwidth usage, epsilon, and distributions

GRAPHING
-----------------
The graph a specific experiment, run:
```
python graphing/bandwidth_vs_time.py -t results\all_osc1_osc2 -a 30
```

This will look in the ```results``` and ```data``` folders, and plot the bandwidth/epsilon/distributions over time. The ```-a``` option specifies what rolling average should be used. The ```-s``` option is used to scale the distribution graph, because we do not use precise enough timing, and the computer is too slow so the other two results will lag from the expected distribution. A simple scaling factor can fix this problem, but should be fixed later.
