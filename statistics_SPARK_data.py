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



inputFile = fullFile
#outFolder = 'yahoo/data/statistics/dev'
outFolder = 'yahoo/data/statistics/full'

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
log.write('inputRating events counted, count is: %d \ntime elapsed is: %d seconds == %d minutes \n\n' % (inputCount,temp_time,temp_time/60))





reducedRDD = inputRDD.reduceByKey(lambda a,b : a + b)

reducedCount = reducedRDD.count()
temp_time=time.time()-start_time
log.write('reduced to (item,count) counted, nr unique items is: %d \ntime elapsed is: %d seconds == %d minutes \n\n' % (reducedCount,temp_time,temp_time/60))


#RUN Distribution counts

filterRDD_1 = reducedRDD.filter(lambda v: v[1] >= 500 and v[1] < 600)
filterCount = filterRDD_1.count()
temp_time=time.time()-start_time
log.write('filtered rdd nr 1 counted, count is: %d, equaling %d percent \ntime elapsed is: %d seconds == %d minutes \n\n' % (filterCount,(filterCount*100)/reducedCount,temp_time,temp_time/60))


filterRDD_2 = reducedRDD.filter(lambda v: v[1] > 600 and v[1] < 700)
filterCount_2 = filterRDD_2.count()
temp_time=time.time()-start_time
log.write('filtered rdd nr 2 counted, count is: %d, equaling %d percent \ntime elapsed is: %d seconds == %d minutes \n\n' % (filterCount_2,(filterCount_2*100)/reducedCount,temp_time,temp_time/60))


filterRDD_3 = reducedRDD.filter(lambda v: v[1] >= 700)
filterCount_3 = filterRDD_3.count()
temp_time=time.time()-start_time
log.write('filtered rdd nr 3 counted, count is: %d, equaling %d percent \ntime elapsed is: %d seconds == %d minutes \n\n' % (filterCount_3,(filterCount_3*100)/reducedCount,temp_time,temp_time/60))


filterRDD_4 = reducedRDD.filter(lambda v: v[1] >= 200)
filterCount_4 = filterRDD_4.count()
temp_time=time.time()-start_time
log.write('filtered rdd nr 4 counted, count is: %d, equaling %d percent \ntime elapsed is: %d seconds == %d minutes \n\n' % (filterCount_4,(filterCount_4*100)/reducedCount,temp_time,temp_time/60))



#---------------------------------------------------
#SAVE a filtered dataset
rddFolder = outFolder +'/files'
filterRDD_4.saveAsTextFile(rddFolder)



end_time=time.time()-start_time
print('All completed, end time is: %d seconds == %d minutes' %(end_time,end_time/60))
log.write('All completed, end time is: %d seconds == %d minutes\n\n' %(end_time,end_time/60))

log.close()
print('\n\n\n\n')
print("#####################################################")
print("#####################################################")





