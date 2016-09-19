
# Usage: python plot-graph-stats.py input-file.json

import sys
import json
import math
import matplotlib.pyplot as plt

def loadJson(fileName):
    return json.loads(open(fileName).read())

def initPlot():
    figWidth=8
    goldenMean=(math.sqrt(5)-1.0)/2.0
    plt.rc('figure', figsize=(figWidth, figWidth*goldenMean))
    plt.rc('figure', autolayout=True)
    plt.rc('font', family='serif')
    plt.rc('font', size=16)
    plt.rc('text', usetex=True)
    plt.rc('legend', fontsize=16)

def plot(x, y, style='k-', markerSize=5, xLog=False, yLog=False):
    if xLog == False:
        if yLog == False: plt.plot(x, y, style, markersize=markerSize)
        else: plt.semilogy(x, y, style, markersize=markerSize)
    else:
        if yLog == False: plt.semilogx(x, y, style, markersize=markerSize)
        else: plt.loglog(x, y, style, markersize=markerSize)

def plotHistogram((jsonLabel, figLabel)):
    hist = data[jsonLabel]["histogram"]
    x = [b for [b, c] in hist]
    y = [float(c) for [b, c] in hist]
    plt.figure()
    plt.xlabel(figLabel)
    plt.ylabel("Count")
    plot(x, y, xLog=True, yLog=True)
    
if __name__ == '__main__' :
    data = loadJson(sys.argv[1])
    labels = [("edge weight", "Edge weight"), ("indegree", "Indegree"), ("outdegree", "Outdegree")]

    initPlot()
    for l in labels: plotHistogram(l)
    plt.show()