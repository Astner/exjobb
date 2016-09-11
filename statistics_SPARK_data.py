#original_data_statistics.py


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



inputFile = devFile
outFolder = 'yahoo/data/statistics/dev'


print("#####################################################")



def extractTrackID(line):
	split = line.split('\t')
	return (split[0],1)


print("#####################################################")


if os.path.isdir(outFolder):
	#os.rmdir(outFolder)
	shutil.rmtree(outFolder)
os.mkdir(outFolder) 



logFile = outFolder + '/LOG_FILE' 
log = open(logFile,'a')

#i = 1

log.write('Initiating on file: ' + inputFile +'\n\n')

print("#####################################################")
print("### Transformations start: ##########################")
ratingFile = sc.textFile(inputFile)


inputRDD = ratingFile.map(lambda line: extractTrackID(line))


inputCount = inputRDD.count()
temp_time=time.time()-start_time
log.write('inputRatings counted, count is: %d \ntime elapsed is: %d seconds == %d minutes \n\n' % (inputCount,temp_time,temp_time/60))





reducedRDD = inputRDD.reduceByKey(lambda a,b : a + b)

reducedCount = reducedRDD.count()
temp_time=time.time()-start_time
log.write('reduced form counted, count is: %d \ntime elapsed is: %d seconds == %d minutes \n\n' % (reducedCount,temp_time,temp_time/60))




filterRDD_1 = reducedRDD.filter(lambda v: v[1] >= 1 and v[1] < 5)
#rdd.filter(lambda x: x % 2 == 0)

filterCount = filterRDD_1.count()
temp_time=time.time()-start_time
log.write('filtered rdd nr 1 counted, count is: %d \ntime elapsed is: %d seconds == %d minutes \n\n' % (filterCount,temp_time,temp_time/60))


filterRDD_2 = reducedRDD.filter(lambda v: v[1] > 5 and v[1] < 10)
#rdd.filter(lambda x: x % 2 == 0)

filterCount_2 = filterRDD_2.count()
temp_time=time.time()-start_time
log.write('filtered rdd nr 2 counted, count is: %d \ntime elapsed is: %d seconds == %d minutes \n\n' % (filterCount_2,temp_time,temp_time/60))





rddFolder = outFolder +'/files'
filterRDD_2.saveAsTextFile(rddFolder)


end_time=time.time()-start_time
print('All completed, end time is: %d seconds == %d minutes' %(end_time,end_time/60))
log.write('All completed, end time is: %d seconds == %d minutes\n\n' %(end_time,end_time/60))

log.close()
print('\n\n\n\n')
print("#####################################################")
print("#####################################################")





