#prediction_targets.py

from pyspark import SparkContext
import os,time
import shutil
import json
from pprint import pprint
import networkx as nx
import numpy as np


sc = SparkContext()



valFile = 'yahoo/data/temp/dev_10_users/validationIdx1_SPARK_VERSION.txt'
valFile_1k = 'yahoo/data/temp/dev_1k_users/validationIdx1_SPARK_VERSION.txt'

fullValFile= 'yahoo/Webscope_C15/ydata-ymusic-kddcup-2011-track1/validationIdx1.txt'

#fullValidation = ''

valRDD = sc.textFile(fullValFile)

def extractKey(line):
	if line == '':
		return []
	split = line.split('\t')
	if len(split) < 2 :
		return []
	else: 
		return ((split[0],1))


uniqueKeysRDD = valRDD.flatMap(lambda line: extractKey(line)).distinct()

uniqueKeysList = uniqueKeysRDD.collect()

#listRDD = uniqueKeysRDD.sortByKey()
#map(lambda k: k[0])




##print('\n\n\n\n')
#print(uniqueKeysRDD.count())
#print('\n\n\n\n')



