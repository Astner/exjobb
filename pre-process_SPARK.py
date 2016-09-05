# run in folder:
# spark-submit spark_test.py

from pyspark import SparkContext
import os,time
import shutil


sc =SparkContext()

print("#####################################################")
print("#####################################################")
print('\n\n\n\n')

print(sc.version)
start_time = time.time()


devFile = 'yahoo/data/temp/testDataset_3_users_SPARK_VERSION.txt'
dev_10k = 'yahoo/data/temp/artistDataset_10k_users_SPARK_VERSION.txt'
dev_100k = 'yahoo/data/temp/artistDataset_100k_users_SPARK_VERSION.txt'
fullFile = 'yahoo/data/trainIdx1_SPARK_VERSION.txt'


ratingFile = sc.textFile(devFile)
outFolder = 'yahoo/data/ngram/3_users'

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


userHistoryRDD = ratingFile.map(lambda line: splitAndRearange(line)).groupByKey()
#print('\n\n\n\n User history created')


print('\n\n\nCount on userHistory: %d' % (userHistoryRDD.count()))
temp_time=time.time()-start_time
print('\n\n\nuserHistory counted, time elapsed is: %d seconds == %d minutes' % (temp_time,temp_time/60))

ngramRDD = userHistoryRDD.flatMap(lambda line: userHistorytoNgram(line))
#print('\n\n\n\n Ngrams created')

print('\n\n\nCount on mgrams: %d' % (ngramRDD.count()))
temp_time=time.time()-start_time
print('\n\n\nngrams counted, time elapsed is: %d seconds == %d minutes' % (temp_time,temp_time/60))



outputRDD = ngramRDD.reduceByKey(lambda a,b : a + b).map(lambda line: mapToOutputFormat(line))
#print('\n\n\n\n Waiting to collect')
#print('\n\n')
#print(outputRDD.collect())
#print('\n')
print('\n\n\nCount on output: %d' % (outputRDD.count()))
temp_time=time.time()-start_time
print('\n\n\nOutput reduction completed, time elapsed is: %d seconds == %d minutes\n\n\n' % (temp_time,temp_time/60))



#shutil.rmtree("dir-you-want-to-remove")
if os.path.isdir(outFolder):
	#os.rmdir(outFolder)
	shutil.rmtree(outFolder)

outputRDD.saveAsTextFile(outFolder)

#Write to single .txt file to match CCM input

#outputList = outputRDD.collect()
#print('\n\n\n Writing to file')

#if os.path.isfile(outFile):
#    os.remove(outFile)    
#out = open(outFile,'a')

#for line in outputList:
#	out.write(line)


end_time=time.time()-start_time
print('All completed, end time is: %d seconds == %d minutes' %(end_time,end_time/60))


print('\n\n\n\n')
print("#####################################################")
print("#####################################################")




