import math
import collections
import numpy as np
import matplotlib.pyplot as plt

def init():
    figWidth=8
    goldenMean=(math.sqrt(5)-1.0)/2.0
    plt.rc('figure', figsize=(figWidth, figWidth*goldenMean))
    plt.rc('figure', autolayout=True)
    plt.rc('font', family='serif')
    plt.rc('font', size=16)
    plt.rc('text', usetex=True)
    plt.rc('legend', fontsize=16)

def plot(xVals, yVals, style = 'o', markerSize = 5, xLog = False, yLog = False):
    if xLog == False:
        if yLog == False: plt.plot(xVals, yVals, style, markersize=markerSize)
        else: plt.semilogy(xVals, yVals, style, markersize=markerSize)
    else:
        if yLog == False: plt.semilogx(xVals, yVals, style, markersize=markerSize)
        else: plt.loglog(xVals, yVals, style, markersize=markerSize)

def bwLinePlot(xVals, yVals, lineStyle = [1], lineWidth = 2, xLog = False, yLog = False):
    if xLog == False:
        if yLog == False: p, = plt.plot(xVals, yVals, 'k-')
        else: p, = plt.semilogy(xVals, yVals, 'k-')
    else:
        if yLog == False: p, = plt.semilogx(xVals, yVals, 'k-')
        else: p, = plt.loglog(xVals, yVals, 'k-')
    p.set_dashes(lineStyle)

def histogram(data, binRange = (0, 1), normalize = False, numBins = 100):
    hist, bins = np.histogram(data, bins=numBins, range=binRange, density=normalize)
    centers = (bins[:-1] + bins[1:]) / 2
    return centers, hist

def cumulativeHistogram(data, normalize = False, numBins = 100):
    x, y = histogram(data, normalize, numBins)
    c = []
    s = 0
    for v in y:
        s += v
        c.append(s)
    return x, c

def counts(data, normalize = False):
    c = collections.defaultdict(int)
    for d in data: c[d] += 1
    p = sorted(c.items(), key=lambda v: v[0])
    k = [a for (a, b) in p]
    v = [b for (a, b) in p]
    if normalize:
        n = float(sum(v))
        v = [float(a)/n for a in v]
    return k, v

def cumulativeCounts(data, normalize = False):
    x, y = counts(data, normalize)
    c = []
    s = 0
    for v in y:
        s += v
        c.append(s)
    return x, c

def labels(xLabel, yLabel):
    plt.xlabel(xLabel)
    plt.ylabel(yLabel)

def legend(labels, location = 0):
    plt.legend(labels, loc=location)

def newFigure():
    plt.figure()
    
def show():
    plt.show()
