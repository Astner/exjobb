#subset-creation.py


from pyspark import SparkContext
import os,time
import shutil

sc = SparkContext()


devFile = 'yahoo/data/temp/testDataset_3_users_SPARK_VERSION.txt'
dev_1k = 'yahoo/data/temp/dev_1k_users/trainIdx1_SPARK_VERSION.txt'
#dev_100k = 'yahoo/data/temp/artistDataset_100k_users_SPARK_VERSION.txt'
fullFile = 'yahoo/data/trainIdx1_SPARK_VERSION.txt'

#valFile = 'yahoo/data/temp/dev_100k_users/validationIdx1_SPARK_VERSION.txt'


val_full = 'yahoo/data/validationIdx1_SPARK_VERSION.txt'
val_1k = 'yahoo/data/temp/dev_1k_users/validationIdx1_SPARK_VERSION.txt'

##############################################
inputFile = fullFile
valFile = val_full
outFolder = 'yahoo/data/temp/subsets/dense'


minItemOccurence = 1200
minUserHistory = 300

#minItemOcc = sc.broadcast(minItemOccurence)
#minUserHist = sc.broadcast(minUserHistory)

print("#####################################################")



def extractTrackID(line):
	split = line.split('\t')
	return (split[0],1)


print("#####################################################")

#Make sure outFolder is an empty folder
if os.path.isdir(outFolder):
	shutil.rmtree(outFolder)
os.mkdir(outFolder) 



#logFile = outFolder + '/LOG_FILE' 
#log = open(logFile,'a')

#i = 1

#log.write('Initiating on file: ' + inputFile +'\n\n')


print("#####################################################")
print("### Transformations start: ##########################")
ratingFile = sc.textFile(inputFile)


inputRDD = ratingFile.map(lambda line: extractTrackID(line))

reducedRDD = inputRDD.reduceByKey(lambda a,b : a + b)

filterRDD = reducedRDD.filter(lambda v: v[1] >= minItemOccurence)

frequentItemRDD = filterRDD.map(lambda x:(x[0],1)) #Extract key, loose count

frequentItems = frequentItemRDD.collectAsMap()

itemDict = sc.broadcast(frequentItems)

nrFreqItems = frequentItemRDD.count()
print('\n\n\n\nNr of frequent items: %d' % (nrFreqItems))



print("\n\n#####################################################")
print("### Frequent items list created: ####################")
print("### Filter-out infrequent events: ###################")


def splitAndRearange(line):
	split = line.split('\t')
	return (split[4],[split[0],split[1],split[2],split[3]])


def filterHistoryLength(userHistory,minCount):
	user = userHistory[0]
	history = userHistory[1]

	length = sum(1 for _ in history)
	if length >= minCount:
		return([(user,1)])
	else:
		return []

print("#####################################################")
print('### Do user history calculations: ###################')


ratingRDD = ratingFile.map(lambda line: splitAndRearange(line))
nrRatings = ratingRDD.count()
print('\n\n\nNr items in ratingRDD: %d' % (nrRatings))

frequentEventsRDD = ratingRDD.filter(lambda line: line[1][0] in itemDict.value)
nrfreqEvents = frequentEventsRDD.count()
print('\n\n\nNr of frequent events: : %d' % (nrfreqEvents))

userFrequentHistoryRDD = frequentEventsRDD.groupByKey()
nrSmallFreqHist = userFrequentHistoryRDD.count()
print('\n\nNr of users after group by Key: %d' % (nrSmallFreqHist))


userLargeFrequentHistoryRDD = userFrequentHistoryRDD.flatMap(lambda user:filterHistoryLength(user,minUserHistory))


nrLargeHistories = userLargeFrequentHistoryRDD.count()
print('\n\nNr of users with large frequent histories: %d' % (nrLargeHistories))


#for item in userLargeFrequentHistoryRDD.collect():
#	print(item)



print("#####################################################")
print('### Do validation data checks: ######################')

validationRDD = sc.textFile(valFile)

frequentValEventsRDD = validationRDD.map(lambda line: splitAndRearange(line))\
									.filter(lambda line: line[1][0] in itemDict.value)\
#Frequent users in form: (userID,1)


nrFreqValEvents = frequentValEventsRDD.count()
print('\n\nNr frequent validation events: %d' % (nrFreqValEvents))


frequentValItemsRDD = frequentValEventsRDD.map(lambda line: (line[0],1)).distinct()

nrFreqValItems = frequentValItemsRDD.count()
print('\n\nNr frequent validation items: %d' % (nrFreqValItems))



#.reduceByKey(lambda a,b: 1)
#									.map(lambda line: (line[0],1))


#for item in userLargeFrequentHistoryRDD.collect():
#	print(item)
##print("#####################################################")
#print("#####################################################")

#for item in frequentValEventsRDD.collect():
#	print(item)


#usefulUserDataRDD = frequentValEventsRDD.groupWith(userLargeFrequentHistoryRDD)


usefulUsersRDD = frequentValItemsRDD.union(userLargeFrequentHistoryRDD)\
									 .reduceByKey(lambda a,b : a + b)\
									 .filter(lambda line: line[1] >= 2)			


#print(usefulUsersRDD.collect())

#for item in usefulUserDataRDD.collect():
#	user = item[0]
#	a = item[1][0]
#	b = item[1][1]
#	print('\n %s' % (user))
#	print('\n A = ')
#	for obj in a:
#		print(obj)
#	print('\n B = ')
##		print(obj)

nrJoined = usefulUsersRDD.count()
print('\n\nNr of useful users in joined RDD: %d' % (nrJoined))


usefulUsers = usefulUsersRDD.collectAsMap()
#.map(lambda line: line[0])

#print(usefulUsers)

#itemDict = sc.broadcast(frequentItems)
userDict = sc.broadcast(usefulUsers)


print("#####################################################")
print("#####################################################")


print('\n\nNr of frequent items: %d' % (nrFreqItems))

print('\n\nNr items in ratingRDD: %d' % (nrRatings))
print('Nr of frequent events: : %d' % (nrfreqEvents))

print('\n\nNr of users with small frequent histories: %d' % (nrSmallFreqHist))

print('Nr of users with large frequent histories: %d' % (nrLargeHistories))

print('\n\nNr frequent validation events: %d' % (nrFreqValEvents))
print('Nr frequent validation items: %d' % (nrFreqValItems))


print('\n\nNr of useful users in union RDD: %d' % (nrJoined))



print("#####################################################")
# Turn usefulUsers + frequent items into a new dataset to use
# Save dataset to disk, should be able to put into pre-process_SPARK.py right away

# Use itemDict and userDict to filter the original datasets



#def splitAndRearange(line):
#	split = line.split('\t')
#	return (split[4],[split[0],split[1],split[2],split[3]])



def rearangeAndMerge(line):
	#outList = list(line[1]).append(line[0])
	#print(outList)
	line[1].append(line[0])
	return('\t'.join(line[1]))

#validationRDD = sc.textFile(valFile)

#print(validationRDD.map(lambda line: splitAndRearange(line)).collect())


denseValEventsRDD = validationRDD.map(lambda line: splitAndRearange(line))\
								  .filter(lambda line: line[1][0] in itemDict.value)\
								  .filter(lambda line: line[0] in userDict.value)\
								  .map(lambda line: rearangeAndMerge(line))

denseTrainEventsRDD = ratingFile.map(lambda line: splitAndRearange(line))\
								.filter(lambda line: line[1][0] in itemDict.value)\
								.filter(lambda line: line[0] in userDict.value)\
								.map(lambda line: rearangeAndMerge(line))



denseValEventsRDD.saveAsTextFile(outFolder +'/validation')
denseTrainEventsRDD.saveAsTextFile(outFolder +'/training')


print(denseTrainEventsRDD.collect())





print("\n\n#####################################################")
print("#####################################################")


