#txt_graph_miner.py


from pyspark import SparkContext
import os,time
import shutil
import json
from pprint import pprint
#import networkx as nx
#import numpy as np
import math
import sys


sc = SparkContext()
print(sc._conf.getAll())


#######################################################################################################
#######################################################################################################

local = False


nrSteps = 2 #max nr steps on graph to find similarity paths
kForKNN = 5


if local: 
    similarityFolder = 'yahoo/data/'
    #localJson = 'yahoo/data/10k_artists_similarities.json'
    localTxt = 'yahoo/data/test_txt.txt'

    #jsonFile = localJson
    txtFile = localTxt

    trainFile = 'yahoo/data/temp/dev_10_users/trainIdx1_SPARK_VERSION.txt'
    valFile = 'yahoo/data/temp/dev_10_users/validationIdx1_SPARK_VERSION.txt'

else:
    sys.path.append('/home/sprk/anaconda3/lib/python3.5/site-packages/')

    similarityFolder = '/extra/data/astner/exjobb/runs/run5/'
    txtFile = similarityFolder + 'similarities.txt'

    trainFile = 'yahoo/data/trainIdx1_SPARK_VERSION.txt'
    valFile = 'yahoo/data/validationIdx1_SPARK_VERSION.txt'
    testFile = 'yahoo/data/testIdx1_SPARK_VERSION.txt'


#######################################################################################################
#######################################################################################################

import networkx as nx


albumFile = '../Webscope_C15/ydata-ymusic-kddcup-2011-track1/albumData1.txt'
trackFile = '../Webscope_C15/ydata-ymusic-kddcup-2011-track1/trackData1.txt'
artistFile = '../Webscope_C15/ydata-ymusic-kddcup-2011-track1/artistData1.txt'
genreFile = '../Webscope_C15/ydata-ymusic-kddcup-2011-track1/genreData1.txt'

#######################################################################################################
#######################################################################################################




print("#####################################################")

def indexToTrackID(index,nodes):
    if 0 <= index and index < len(nodes):
        return int(nodes[index]['name'])
    else: 
        print('Error: index not in Nodes')
        return 0


# def createGraphFromJSON(jsonFile):
#     with open(jsonFile) as data_file:    
#         data = json.load(data_file)
#     nodes = data['nodes'] #719 for 10k_users
#     links = data['links'] #1000 for 10k_users
#     del data #Free space   

#     G = nx.Graph() #Create Graph object G from JSON data, G is symmetrical

#     for node in nodes:  
#         #  Structure ex: {"name" : "20737", "group" : 0.0}
#         G.add_node(int(node['name']))
    
#     for link in links:
#         # Example structure: {"source" : 9,"target" : 0,"value" : 0.9920000000000001}   
#     	source = indexToTrackID(link['source'],nodes)
#     	target = indexToTrackID(link['target'],nodes)
#     	sim = float(link['value'])
#     	dist = 1 - sim
#     	if sim > 0 and dist > 0: 
# 	    	G.add_edge(source,target,similarity = sim, distance = dist)

#     nx.freeze(G)
#     return G


def createGraphFromTXT(txtFile):
	f = open(txtFile) #Open file with read only permits
	line = f.readline() #read first line

	G = nx.Graph() #Create Graph object G from JSON data, G is symmetrical
	
	# If the file is not empty keep reading line one at a time
	# till the file is empty
	while line != '\n':
		link = line.strip('\n').split(' ') #Turn to usable link format
		weight = float(link[0])
		source = int(link[1])
		target = int(link[2])

		dist = 1 - weight

		print('Weight: %.12f, From: %d, To: %d' % (weight,source,target))

		#TODO: INDEX TO TRACK ID?????
		if weight > 0 and dist > 0: 
			G.add_edge(source,target,similarity = weight, distance = dist)

		line = f.readline()
	
	f.close() #Clean up
	nx.freeze(G) #Turn Immutable

	return G

def splitAndLabel(line,label):
    split = line.split('\t')
    if len(split) == 5:
        return [(split[4],[split[0],split[1],label])]
    else:
        return []


def userToPredObject(line):
	userID,values = line
	history = []
	validation = []
	for item in values:
		if item[2] == 'history':
			history.append([item[0],item[1]])
		elif item[2] == 'validation':
			validation.append([item[0],item[1]])
	return 0

print("#####################################################")

#############################################
#Create graph and broadcast it

graph = createGraphFromTXT(txtFile)

print(graph.nodes())

nrEdges = len(graph.edges())
nrNodes = len(graph.nodes())
print('\n\n\n\nNr of edges: %d' % (nrEdges))
print('\n\n\n\nNr of nodes: %d' % (nrNodes))

G = sc.broadcast(graph)
#Use: G.value to access graph

#############################################
#Open files

trainRDD = sc.textFile(trainFile)
#testRDD = sc.textFile(testFile)
valRDD = sc.textFile(valFile)

nrTrainItems = trainRDD.count()
nrValItems = valRDD.count()
print('\n\nNr of items in trainRDD: %d \n\n' % (nrTrainItems))
print('\n\nNr of items in validationRDD: %d \n\n' % (nrValItems))


print('\n\n\n\n')
print("#####################################################")


def extractKey(line):
    if line == '':
        return []
    split = line.split('\t')
    if len(split) < 2 :
        return []
    else: 
        return [(int(split[0]),None)]


uniqueTrainKeysRDD = trainRDD.flatMap(lambda line: extractKey(line)).distinct().map(lambda v: v[0])
uniqueValKeysRDD = valRDD.flatMap(lambda line: extractKey(line)).distinct().map(lambda v: v[0])


#nrUniqueValKeys = uniqueValKeysRDD.count()
#nrUniqueTrainKeys = uniqueTrainKeysRDD.count()
#print('\n\nNr of items in unique validation keys RDD: %d \n\n' % (nrUniqueValKeys))
#print('\n\nNr of items in unique training keys RDD: %d \n\n' % (nrUniqueTrainKeys))

print('\n\n\n\n')
print("#####################################################")
#For each unique key, run all paors shortest paths on G

def aggregateSimilarities(simList):
    ans = 1    
    if(len(simList) < 1):
        #print('ERROR: too few elements in similarity list')
        return 0
    for i in range(0,len(simList)):
        ans = ans * simList[i]
    return ans #TODO: use log(ans) here????

#PRE: path exists
# from list -> float
def pathToSimilarity(G,path):
    steps = len(path) - 1
    similarities = [None] * steps
    
    if steps < 1:
        #It is one item compared to itself
        return 1
    for i in range(0,steps):
        similarities[i] = G.value[path[i]][path[i+1]]['similarity']
        
    return aggregateSimilarities(similarities)


def existsInGraph(node,G):
    if node in G.value:
        return((node,True))
    else:
        return((node,False))


#def shortestPaths(node,G,depth):

print('\n\n\n\n')
print("#####################################################")
# Create iser objects

trainUserRDD = trainRDD.flatMap(lambda line: splitAndLabel(line,'history'))
valUserRDD = valRDD.flatMap(lambda line: splitAndLabel(line,'validation'))

usersRDD = trainUserRDD.union(valUserRDD).groupByKey()




uniqueTrainKeysRDD = trainRDD.flatMap(lambda line: extractKey(line)).distinct().map(lambda v: v[0])
uniqueValKeysRDD = valRDD.flatMap(lambda line: extractKey(line)).distinct().map(lambda v: v[0])

nrValHits = uniqueValKeysRDD.map(lambda key: existsInGraph(key,G)).filter(lambda v: v[1] == True).count()
nrTrainHits = uniqueTrainKeysRDD.map(lambda key: existsInGraph(key,G)).filter(lambda v: v[1] == True).count()

print('\n\nNr of validation keys in graph: %d' % (nrValHits))
print('\n\nNr of training keys in graph: %d' % (nrTrainHits))








