#!/usr/bin/python

import sys
import math
import json
import plotlib as pl
import matplotlib.pyplot as plt
import scipy.stats as sps
import os

def loadJSON(fileName):
    return json.loads(open(fileName).read())

def getLevel0Prop(data, prop):
    return [r[prop] for r in data["results"]]

def plotVsMaxInDegree(data, prop, label):
    pl.newFigure()
    pl.plot(getLevel0Prop(data, "max in-degree"), getLevel0Prop(data, prop), style = 'k-o', markerSize = 4)
    pl.labels("In-degree threshold", label)

def level0Plot(data, (prop1, label1), (prop2, label2)):
    fig = pl.newFigure()
    pl.plot(getLevel0Prop(data, prop1), getLevel0Prop(data, prop2), style = 'k-o', markerSize = 4)
    pl.labels(label1, label2)
    return fig

def histograms(data, prop, label):
    pl.newFigure()
    for d in data["results"]:
        hist = d[prop]["histogram"]
        bins = [b for [b, c] in hist]
        counts = [float(c) for [b, c] in hist]
        deltaTot = sum(counts) * (bins[1] - bins[0])
        pl.plot(bins, [c/deltaTot for c in counts], style = '-', xLog = False, yLog = False)
    pl.labels(label, "Density")
    pl.legend(getLevel0Prop(data, "max in-degree"))

def similarityDistribution(data):
    pl.newFigure()
    for d in data["results"]:
        hist = d["relative l1 norm"]["histogram"]
        bins = [1.0 - b for [b, c] in hist]
        counts = [float(c) for [t, c] in hist]
        tot = sum(counts)
        pl.plot(bins, [c/tot for c in counts], style = '-', xLog = True, yLog = True)
    pl.labels("$1 - \mathrm{L_{1}}$", "Density")
    pl.legend(getLevel0Prop(data, "max in-degree"))

def level1Stats(data, prop):
    stats = [d[prop] for d in data["results"]]
    means = [s["mean"] for s in stats]
    stdDevs = [s["standard deviation"] for s in stats]
    sums = [s["sum"] for s in stats]
    return means, stdDevs, sums

def plotLevel0VsLevel1Stats(data, prop0, prop1, label0, label1):
    d0 = getLevel0Prop(data, prop0)
    m, std, sm = level1Stats(data, prop1)
    for (v, p) in [(m, "mean"), (std, "standard deviation"), (sm, "sum")]:
        pl.newFigure()
        pl.plot(d0, v, style = 'ko-', xLog = False, yLog = False)
        pl.labels(label0, label1 + " " + p)

def selectedSimilarities(data):
    def sims(run):
        relations = run["selected concept relations"]
        def getSim(r):
            [w1, w2] = r["pair"].split(",")
            l1 = r["relative l1 norm"]
            if w1 < w2: return ((w1, w2), 1.0 - l1)
            return ((w2, w1), 1.0 - l1)
        return [getSim(r) for r in relations]
    return [sims(run) for run in data["results"]]

def loadBenchmark(file):
    def parse(line):
        [w1, w2, s] = line.split(",")
        s = float(s.strip())
        if w1 < w2: return ((w1, w2), s)
        return ((w2, w1), s)
    return [parse(line) for line in open(file)]

def intersectionSimilarities(set1, set2):
    def dictAndPairs(s): return dict(s), set([p for (p, v) in s])
    dict1, pairs1 = dictAndPairs(set1)
    dict2, pairs2 = dictAndPairs(set2)
    sharedPairs = pairs1.intersection(pairs2)
    sims1 = [dict1[p] for p in sharedPairs]
    sims2 = [dict2[p] for p in sharedPairs]
    return sims1, sims2

def wordSimScatterPlot(set1, set2, label1, label2):
    pl.newFigure()
    sims1, sims2 = intersectionSimilarities(set1, set2)
    pl.plot(sims1, sims2, style = 'ko', markerSize = 4)
    pl.labels(label1, label2)
    print "Spearman correlation between", label1, "and", label2, ":", spearmanCorrelation(sims1, sims2)

def spearmanCorrelation(v1, v2):
    return sps.spearmanr(v1, v2)[0]

def correlationsVsLevel0Prop(data, prop0, label0, benchmarks, benchLabels):
    pl.newFigure()
    l0Prop = getLevel0Prop(data, prop0)
    l1Sims = selectedSimilarities(data)

    for b in benchmarks:
        def spearman(s1, s2):
            sims1, sims2 = intersectionSimilarities(s1, s2)
            return spearmanCorrelation(sims1, sims2)
        correlations = [spearman(b, s1) for s1 in l1Sims]
        pl.plot(l0Prop, correlations, style = 'o-', markerSize = 4)

    pl.labels(label0, "Spearman rank correlation coefficient")
    pl.legend(benchLabels)

if __name__ == '__main__' :

    # If show is set to true figures will not be saved, just shown
    show = False
    experiment_name = "eng-all-edge-low-e3-vtx-high-e1"
    if len(sys.argv) == 2:
        experiment_name = sys.argv[1]
    experiment_file = experiment_name + ".json"
    experimentPath = "../data/experiments/"
    benchmarkPath = "../data/benchmarks/"
    figurePath = "../figures/" + experiment_name
    # Create figure path if it doesn't exist
    if not os.path.exists(figurePath):
        os.makedirs(figurePath)

    data = loadJSON(experimentPath + experiment_file)
    mt287 = loadBenchmark(benchmarkPath + "mturk-287.txt")
    mt771 = loadBenchmark(benchmarkPath + "mturk-771.txt")
    ws353 = loadBenchmark(benchmarkPath + "ws-353.txt")
    simlex999 = loadBenchmark(benchmarkPath + "simlex.txt")

    # Level 0 properties
    md = ("max in-degree", "In-degree threshold")
    rt = ("runtime", "Runtime [s]")
    nv = ("number of vertices", "Number of vertices")
    ne = ("number of edges", "Number of edges")
    nr = ("number of relations", "Number of similarity relations")

    # Similarities at highest in-degree threshold
    s1 = selectedSimilarities(data)[-1]

    pl.init()
    wordSimScatterPlot(s1, ws353, "$1 - L_1$", "WS-353")
    wordSimScatterPlot(s1, mt287, "$1 - L_1$", "MT-287")
    wordSimScatterPlot(s1, mt771, "$1 - L_1$", "MT-771")
    wordSimScatterPlot(s1, simlex999, "$1 - L_1$", "SIMLEX-999")
    level0Plot(data, md, rt)
    level0Plot(data, md, nv)
    level0Plot(data, md, ne)
    histograms(data, "relative l1 norm", "Relative $\mathrm{L_{1}}$ norm")
    histograms(data, "max error", "Error bound")
    plotLevel0VsLevel1Stats(data, "max in-degree", "relative l1 norm", "In-degree threshold", "Relative $\mathrm{L_{1}}$ norm")
    plotLevel0VsLevel1Stats(data, "max in-degree", "max error", "In-degree threshold", "Error bound")
    correlationsVsLevel0Prop(data, "max in-degree", "In-degree threshold", [ws353, mt287, mt771, simlex999], ["WS-353", "MT-287", "MT-771", "SIMLEX-999"])
    if show:
        pl.show()

    # Save figures under figure path, will not work if figures have already been displayed with .show()
    for i in plt.get_fignums():
        plt.figure(i)
        plt.savefig(figurePath + '/' + experiment_name + '-figure%d.pdf' % i)
