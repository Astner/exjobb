# run in folder:
# spark-submit spark_test.py

from pyspark import SparkContext
import os,time
import shutil


sc = SparkContext()


print("#####################################################")
print("#####################################################")
print('\n\n\n\n')

print(sc.version)
start_time = time.time()


devFile = 'yahoo/data/temp/testDataset_3_users_SPARK_VERSION.txt'
dev_10k = 'yahoo/data/temp/artistDataset_10k_users_SPARK_VERSION.txt'
dev_100k = 'yahoo/data/temp/artistDataset_100k_users_SPARK_VERSION.txt'
fullFile = 'yahoo/data/trainIdx1_SPARK_VERSION.txt'

#TODO: add split versions and paths
#ratingFile = sc.textFile(devFile)


#inputPath = 'yahoo/data/temp'
#dev_1k = 'yahoo/data/temp/split_dataset_1k_users'
#dev_10k = 'yahoo/data/temp/split_dataset_10k_users'
#dev_100k = 'yahoo/data/temp/split_dataset_100k_users'
#dev_all = 'yahoo/data/temp/split_dataset_ALL_users'


denseSubset = 'yahoo/data/temp/subsets/dense/training'


#inputFolder = dev_100k
inputFile = denseSubset
outFolder = 'yahoo/data/ngram/denseSub'




#outFile = 'yahoo/data/ngram/tempNRAM_3_users.txt'

print("#####################################################")
#print('Count on ratingFile: %s ' % (ratingFile.count()))

#print(ratingFile.first())
#print(ratingFile.collect())

#flatMap(lambda line: line.split())

def splitAndRearange(line):
	split = line.split('\t')
	return (split[4],[split[0],split[1],split[2]])


def userHistorytoNgram(line):
	(user,itemList) = line
	out = []
	#iter2 = iterable

	for line1 in itemList:
		for line2 in itemList:
			if line1[1] == line2[1]: #Checking RATING criteria
				if line1[2] == line2[2]: #Checking DATE criteria 
					if line1 != line2: 
						out.append(((line1[0],line2[0]),1))
	return out


def mapToOutputFormat(line):
	((k1,k2),count) = line
	return str(count)+','+k1+','+k2

print("#####################################################")

#tabMapped = ratingFile.map(lambda line: line.split('\t')) 
# Example of one line after tabsplit:
# ['115200', '100', '4220', '10:16:00', '2']

#print(tabMapped.collect())
#print('\n')

#tempRDD = tabMapped.map(lambda line: (line[4],[line[0],line[1],line[2]]))

#Make sure main folder is empty and existing
if os.path.isdir(outFolder):
	#os.rmdir(outFolder)
	shutil.rmtree(outFolder)
os.mkdir(outFolder) 

logFile = outFolder + '/LOG_FILE' 
log = open(logFile,'a')

#i = 1

log.write('Initiating on file: ' + inputFile +'\n\n')

#import os
#List all files in the folder & make sure they end in .txt
#for file in os.listdir(inputFolder):
#	if file.endswith(".txt"):
#		print(file)

ratingFile = sc.textFile(inputFile)

userHistoryRDD = ratingFile.map(lambda line: splitAndRearange(line)).groupByKey()


userCount = userHistoryRDD.count()
#print('\n\n\n\n User history created')
print('\n\n\nCount on userHistory: %d' % (userCount))
temp_time=time.time()-start_time
log.write('userHistory counted, count is: %d \ntime elapsed is: %d seconds == %d minutes \n\n' % (userCount,temp_time,temp_time/60))




ngramRDD = userHistoryRDD.flatMap(lambda line: userHistorytoNgram(line))

#print('\n\n\n\n Ngrams created')
ngramCount = ngramRDD.count()
print('\n\n\nCount on ngrams: %d' % (ngramCount))
temp_time=time.time()-start_time
log.write('ngrams counted,  count is: %d \ntime elapsed is: %d seconds == %d minutes\n\n' % (ngramCount,temp_time,temp_time/60))


outputRDD = ngramRDD.reduceByKey(lambda a,b : a + b).map(lambda line: mapToOutputFormat(line))

outCount = outputRDD.count()
print('\n\n\nCount on outputs: %d' % (outCount))
temp_time=time.time()-start_time
log.write('output created, count is: %d \ntime elapsed is: %d seconds == %d minutes\n\n' % (outCount,temp_time,temp_time/60))

#if os.path.isdir(outFolder):
	#os.rmdir(outFolder)
#	shutil.rmtree(outFolder)


rddFolder = outFolder +'/files'
outputRDD.saveAsTextFile(rddFolder)



end_time=time.time()-start_time
print('All completed, end time is: %d seconds == %d minutes' %(end_time,end_time/60))
log.write('All completed, end time is: %d seconds == %d minutes\n\n' %(end_time,end_time/60))

log.close()
print('\n\n\n\n')
print("#####################################################")
print("#####################################################")


#78 min...

