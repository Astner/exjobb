#graph_miner_SPARK.py


from pyspark import SparkContext
import os,time
#import shutil
import json
#from pprint import pprint
#import networkx as nx
import numpy as np



sc = SparkContext()

#file = 'yahoo/data/10k_artists_similarities_v1.json'
jsonFile = 'yahoo/data/10k_artists_similarities.json'

trainFile = 'yahoo/data/temp/dev_10_users/trainIdx1_SPARK_VERSION.txt'
valFile = 'yahoo/data/temp/dev_10_users/validationIdx1_SPARK_VERSION.txt'
testFile = 'yahoo/data/temp/dev_10_users/testIdx1_SPARK_VERSION.txt'

print("#####################################################")




#shortestPaths(landmarks)
#Runs the shortest path algorithm from a set of landmark vertices in the graph.
#See Scala documentation for more details.
#Parameters:	landmarks – a set of one or more landmarks
#Returns:	DataFrame with new vertices column “distances”



