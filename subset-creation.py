#subset-creation.py


from pyspark import SparkContext
import os,time

sc = SparkContext()


devFile = 'yahoo/data/temp/testDataset_3_users_SPARK_VERSION.txt'
dev_1k = 'yahoo/data/temp/dev_1k_users/trainIdx1_SPARK_VERSION.txt'
#dev_100k = 'yahoo/data/temp/artistDataset_100k_users_SPARK_VERSION.txt'
fullFile = 'yahoo/data/trainIdx1_SPARK_VERSION.txt'

#valFile = 'yahoo/data/temp/dev_100k_users/validationIdx1_SPARK_VERSION.txt'


fullValFile = 'yahoo/data/validationIdx1_SPARK_VERSION.txt'
val_1k = 'yahoo/data/temp/dev_1k_users/validationIdx1_SPARK_VERSION.txt'

##############################################
inputFile = devFile
valFile = val_1k
#outFolder = 'yahoo/data/ngram/dev'


minItemOccurence = 2
minUserHistory = 3

#minItemOcc = sc.broadcast(minItemOccurence)
#minUserHist = sc.broadcast(minUserHistory)

print("#####################################################")



def extractTrackID(line):
	split = line.split('\t')
	return (split[0],1)


print("#####################################################")

#if os.path.isdir(outFolder):
#	shutil.rmtree(outFolder)
#os.mkdir(outFolder) 

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
	return (split[4],[split[0],split[1],split[2]])


def filterHistoryLength(userHistory,minCount):
	#user = userHistory[0]
	history = userHistory[1]

	length = sum(1 for _ in history)
	if length >= minCount:
		return([(userHistory)])
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
									.filter(lambda line: line[1][0] in itemDict.value)


for item in userLargeFrequentHistoryRDD.collect():
	print(item)
print("#####################################################")
print("#####################################################")

for item in frequentValEventsRDD.collect():
	print(item)


usefulUserDataRDD = frequentValEventsRDD.join(userLargeFrequentHistoryRDD)

nrJoined = validationRDD.count()
print('\n\nNr items in joined RDD: %d' % (nrJoined))



print('\n\nNr of frequent items: %d' % (nrFreqItems))

print('\n\nNr items in ratingRDD: %d' % (nrRatings))
print('Nr of frequent events: : %d' % (nrfreqEvents))

print('\n\nNr of users with small frequent histories: %d' % (nrSmallFreqHist))

print('Nr of users with large frequent histories: %d' % (nrLargeHistories))

print('\n\nNr items in joined RDD: %d' % (nrJoined))


#oneHistory = userHistoryRDD.collect()[0][1] 
#print('\n\n\n History length: %d' % (sum(1 for _ in oneHistory)))

#for item in oneHistory:
#	print(item)


#trainUserRDD = trainRDD.flatMap(lambda line: splitAndLabel(line,'history'))
#valUserRDD = valRDD.flatMap(lambda line: splitAndLabel(line,'validation'))

#print('\n\nNr of items in train: %d \n\n' % (trainUserRDD.count()))
#print('\n\nNr of items in validation: %d \n\n' % (valUserRDD.count()))

#usersRDD = trainUserRDD.union(valUserRDD).groupByKey()




print("#####################################################")




#inputCount = inputRDD.count()
#temp_time=time.time()-start_time
#log.write('inputRating events counted, count is: %d \ntime elapsed is: %d seconds == %d minutes \n\n' % (inputCount,temp_time,temp_time/60))




#---------

#Read dataset

#Read file with list of frequent songs

#Remove every item not in frequent itemss

#Remove all users with no predict data or ti small history


#Save dataset

