#graph_test.py


from pyspark import SparkContext
import os,time
import shutil
import json
from pprint import pprint
import networkx as nx
import numpy as np



sc = SparkContext()

#file = 'yahoo/data/10k_artists_similarities_v1.json'
localJson = 'yahoo/data/10k_artists_similarities.json'
iceJson = 'concepts-spark20-refactor/experimental/higherorder/data_100k_mcc3/similarities.json'

jsonFile = localJson

trainFile = 'yahoo/data/temp/dev_10_users/trainIdx1_SPARK_VERSION.txt'
valFile = 'yahoo/data/temp/dev_10_users/validationIdx1_SPARK_VERSION.txt'
testFile = 'yahoo/data/temp/dev_10_users/testIdx1_SPARK_VERSION.txt'

#trainFile = 'yahoo/data/temp/dev_100k_users/trainIdx1_SPARK_VERSION.txt'
#valFile = 'yahoo/data/temp/dev_100k_users/validationIdx1_SPARK_VERSION.txt'


print("#####################################################")

def indexToTrackID(index,nodes):
    if 0 <= index and index < len(nodes):
        return int(nodes[index]['name'])
    else: 
        print('Error: index not in Nodes')
        return 0


def createGraphFromJSON(jsonFile):
    with open(jsonFile) as data_file:    
        data = json.load(data_file)
    nodes = data['nodes'] #719 for 10k_users
    links = data['links'] #1000 for 10k_users
    del data #Free space   

    G = nx.Graph() #Create Graph object G from JSON data, G is symmetrical

    for node in nodes:  
        #  Structure ex: {"name" : "20737", "group" : 0.0}
        G.add_node(int(node['name']))
    
    for link in links:
        # Example structure: {"source" : 9,"target" : 0,"value" : 0.9920000000000001}   
    	source = indexToTrackID(link['source'],nodes)
    	target = indexToTrackID(link['target'],nodes)
    	sim = float(link['value'])
    	dist = 1 - sim
    	if sim > 0 and dist > 0: 
	    	G.add_edge(source,target,similarity = sim, distance = dist)

    nx.freeze(G)
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
graph = createGraphFromJSON(jsonFile)
#b = sc.broadcast([1, 2, 3, 4, 5])
G = sc.broadcast(graph)
#Use: G.value to access graph

#print(graph.edges(data='distance'))
print('Nr of edges: %d' % (len(graph.edges())))
print('Nr of nodes: %d' % (len(graph.nodes())))

#def all_pairs_dijkstra_path(G, cutoff=None, weight='weight'):
#   """ Compute shortest paths between all nodes in a weighted graph.

#print('\n\n\n\nStarting graph shortest paths: \n\n\n')
#tart_time = time.time()
#paths = nx.all_pairs_dijkstra_path(graph,cutoff = 2,weight = 'distance')
#end_time=time.time()-start_time
#print('Paths completed, end time is: %d seconds == %d minutes' %(end_time,end_time/60))
#print('Size of paths object: %d' % (len(paths)))
#path = [218079, 475789, 458576, 107772, 279203, 263935]
#for item in path:
#	print(item in G.value)

#print(G[path[0]])
#print('\n\n\n')
#nodeValues = sc.parallelize(path).map(lambda x: graph.value[x])
#print('\n\n Count on nodeValues: %d \n\n' %(nodeValues.count()))
#collected = nodeValues.collect()
#print(collected[0])





#Create user objects from input files / 1 predict object for each target item

trainRDD = sc.textFile(trainFile)
#testRDD = sc.textFile(testFile)
valRDD = sc.textFile(valFile)

print('\n\nNr of items in trainRDD: %d \n\n' % (trainRDD.count()))
print('\n\nNr of items in validationRDD: %d \n\n' % (valRDD.count()))

print('\n\n\n\n')
print("#####################################################")

#valRDD = sc.textFile(fullValFile)

def extractKey(line):
    if line == '':
        return []
    split = line.split('\t')
    if len(split) < 2 :
        return []
    else: 
        return ((split[0]),None)


uniqueTrainKeysRDD = trainRDD.flatMap(lambda line: extractKey(line)).distinct()
uniqueValKeysRDD = valRDD.flatMap(lambda line: extractKey(line)).distinct()
print('\n\nNr of items in unique validation keys RDD: %d \n\n' % (uniqueValKeysRDD.count()))
print('\n\nNr of items in unique training keys RDD: %d \n\n' % (uniqueTrainKeysRDD.count()))
#print(uniqueValKeysRDD.collect())

#uniqueKeysList = uniqueKeysRDD.collect()


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
    return ans

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


def shortestPaths(node,G,depth):
    #[docs]def single_source_dijkstra(G, source, target=None, cutoff=None, weight='weight'):
    #"""Compute shortest paths and lengths in a weighted graph G.

    if node in G.value:
        #print('\n\n We got a node')
        paths = nx.single_source_dijkstra_path(G.value,node,cutoff = depth,weight = 'distance') 
        #Turn paths to similarities

        #print(paths)
        #print('\n\n We got a path\n\n\n')
        #print(paths)
        #print('\n\nLength of paths = %d' % (len(paths)))

        if len(paths) > 0:

            for target in paths:
                #print(target)
                path = paths[target]
                similarity = pathToSimilarity(G,path)
                paths[target] = similarity
        else:
            #No paths found, should be pruned but is defensive
            paths = None
    else:
        #the node is not in the graph
        paths = None
    #{48824: [218079, 48824],
    # 149530: [218079, 475789, 149530],
    #168335: [218079, 48824, 168335],
    # 173380: [218079, 475789, 173380],
    #218079: [218079],
    # 222849: [218079, 475789, 222849],
    # acess: paths[targetID] -> [source,nodes,target]

    # Turn path to similarity?
#    return (node,paths)

    #print('\n\n\n')
    #print((node,paths))
    return (node,paths)

def existsInGraph(node,G):

    if node in G.value:
        return((node,True))
    else:
        return((node,False))

print('\n\n\n\n')
print("#####################################################")
print("### Run hit statistics on graph #####################")

nrValHits = uniqueValKeysRDD.map(lambda key: existsInGraph(key,G)).filter(lambda v: v[1] == True).count()
nrTrainHits = uniqueTrainKeysRDD.map(lambda key: existsInGraph(key,G)).filter(lambda v: v[1] == True).count()

print('\n\nNr of validation keys in graph: %d' % (nrValHits))
print('\n\nNr of training keys in graph: %d' % (nrTrainHits))

#
nrSteps = 4
spRDD = uniqueValKeysRDD.map(lambda key: shortestPaths(key,G,nrSteps))

print('\n\nNr of items in shortest paths RDD: %d \n\n' % (spRDD.count()))
print('\n\nNr of NON-empty items in shortest paths RDD: %d \n\n' % (spRDD.filter(lambda v: v[1] != None).count()))

    

spDict = spRDD.collectAsMap()
simDict = sc.broadcast(spDict)
# Access: paths.value[itemID] -> sim
#drop broadcast?
G.destroy() #No more need for the graph, everything is extracted as similarities



trainUserRDD = trainRDD.flatMap(lambda line: splitAndLabel(line,'history'))
valUserRDD = valRDD.flatMap(lambda line: splitAndLabel(line,'validation'))

print('\n\nNr of items in train: %d \n\n' % (trainUserRDD.count()))
print('\n\nNr of items in validation: %d \n\n' % (valUserRDD.count()))


usersRDD = trainUserRDD.union(valUserRDD).groupByKey()


print('\n\nNr of items in userRDD: %d \n\n' % (usersRDD.count()))
#k,v = usersRDD.collect()[0]
#
#for item in v:
#	print(item)


#def has_path(G, source, target):
#try:
#        sp = nx.shortest_path(G,source, target)
#    except nx.NetworkXNoPath:
#        return False
#    return True



#[docs]def single_source_dijkstra(G, source, target=None, cutoff=None, weight='weight'):
#    """Compute shortest paths and lengths in a weighted graph G.



#Map each predict task, with Graph as broadcasted variable
# - Calculate similarities
# - Do ML algorithm

print('\n\n\n\n')
print("#####################################################")
print("#####################################################")

