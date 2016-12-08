#subset-creation_2.py


from pyspark import SparkContext
import os,time
import shutil

sc = SparkContext()


#devFile = 'yahoo/data/temp/testDataset_3_users_SPARK_VERSION.txt'
dev_1k = 'yahoo/data/temp/dev_1k_users/trainIdx1_SPARK_VERSION.txt'
fullFile = 'yahoo/data/trainIdx1_SPARK_VERSION.txt'


val_full = 'yahoo/data/validationIdx1_SPARK_VERSION.txt'
val_1k = 'yahoo/data/temp/dev_1k_users/validationIdx1_SPARK_VERSION.txt'

testFull = ''

##############################################
inputFile = dev_1k
valFile = val_1k
testFile = testFull
outFolder = 'yahoo/data/temp/subsets/small'


#minItemOccurence = 1200
minUserHistory = 10
nrToSkip = 5 #One in every nrToSkip will be slected to be used in subset


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


print("#####################################################")
print("### Transformations start: ##########################")
ratingFile = sc.textFile(inputFile)


inputRDD = ratingFile.map(lambda line: extractTrackID(line))

reducedRDD = inputRDD.reduceByKey(lambda a,b : a + b).map(lambda a: (a[1],a[0]))

longlist = sorted(reducedRDD.collect())

print('\n\n\nFull long list length: %d' % (len(longlist)))
#print(longlist[0:20])

#Remove every second item from the sorted list
j = 0
for i in range(0,len(longlist)):
	if i % 5 == 0:
		longlist[j] = int(longlist[j][1])
		j = j + 1
	else:
		del longlist[j]
		

print('\n\n\nShortened long list length: %d' % (len(longlist)))
print(longlist[0:10])


itemDict = {}
for item in longlist:
	itemDict[int(item)] = 1

itemList = sc.broadcast(itemDict)


#print(itemList.value)

############################################################

#ratingFile is already defined
validationRDD = sc.textFile(valFile)
testRDD = sc.textFile(testFile)

#Extract the userID to be key for touple
def splitAndRearange(line):
	split = line.split('\t')
	return (split[4],[split[0],split[1],split[2],split[3]])


# Target Form: (songID,[rating,datet,time,songID])
def splitPreserveOrder(line):
	split = line.split('\t')
	return (split[0],[split[1],split[2],split[3],split[4]])

# Target Form: (userID,[songID,rating,datet,time])
def shuffleToUserID(line):
	return (line[1][3],[line[0],line[1][0],line[1][1],line[1][2]])

def extractUserID(line):
	return (int(line[1][3]),1)

##########################################################
# Filter events to only include selected subset of items


filteredTraining 	= ratingFile.map(lambda v: splitPreserveOrder(v)).filter(lambda v: int(v[0]) in itemList.value)
filteredValidation 	= validationRDD.map(lambda v: splitPreserveOrder(v)).filter(lambda v: int(v[0]) in itemList.value)
filteredTest 		= testRDD.map(lambda v: splitPreserveOrder(v)).filter(lambda v: int(v[0]) in itemList.value)


print('\n\n\n\nNr of items in dataset: %d' % (ratingFile.count()))
print('\n\n\n\nNr train items after filtering: %d' % (filteredTraining.count()))
print('\n\n\n\nNr validation items after first filtering: %d' % (filteredValidation.count()))

##########################################################
# Filter validation and test for users with large enough history


usefulUsersRDD = filteredTraining.map(lambda v: extractUserID(v))\
								.reduceByKey(lambda a,b:a + b).filter(lambda v: v[1] >= minUserHistory)


usersDict = usefulUsersRDD.collectAsMap()
userList = sc.broadcast(usersDict)


usefulValidationsRDD 	= filteredValidation.filter(lambda line: int(line[1][3]) in userList.value)
usefulTestssRDD 		= filteredTest.filter(lambda line: int(line[1][3]) in userList.value)


#Do not take into account any minimum limits on nr in validation (except existanse)

print('\n\n\n\nNr of useful users after filtering: %d' % (len(usersDict)))

print('\n\n\n\nNr of useful validation items after double filtering: %d' % (usefulValidationsRDD.count()))
#print('\n\n\n\nNr of useful test items after double filtering: %d' % (usefulTestsRDD.count()))

##################################################################
# Write new subset to file 


# Input Form: (songID,[rating,datet,time,songID])
def mergeFromSplit(line):
	l = [line[0]] + line[1]
	return('\t'.join(l))

usefulValidationsRDD.map(lambda line: mergeFromSplit(line)).saveAsTextFile(outFolder +'/validation')
#usefulTestssRDD.map(lambda line: mergeFromSplit(line)).saveAsTextFile(outFolder +'/test')
filteredTraining.map(lambda line: mergeFromSplit(line)).saveAsTextFile(outFolder +'/training')








