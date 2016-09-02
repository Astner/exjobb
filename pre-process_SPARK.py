# run in folder:
# spark-submit spark_test.py

from pyspark import SparkContext
import os,time

sc =SparkContext()

print("#####################################################")
print("#####################################################")
print('\n\n\n\n')

print(sc.version)


devFile = 'yahoo/data/temp/testDataset_3_users_SPARK_VERSION.txt'

dev_10k = 'yahoo/data/temp/artistDataset_10000_users_SPARK_VERSION.txt'

ratingFile = sc.textFile(devFile)


outFile = 'yahoo/data/ngram/tempNRAM_4users.txt'


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
	return str(count)+','+k1+','+k2+'\n'

print("#####################################################")

#tabMapped = ratingFile.map(lambda line: line.split('\t')) 
# Example of one line after tabsplit:
# ['115200', '100', '4220', '10:16:00', '2']

#print(tabMapped.collect())
#print('\n')

#tempRDD = tabMapped.map(lambda line: (line[4],[line[0],line[1],line[2]]))


userHistoryRDD = ratingFile.map(lambda line: splitAndRearange(line)).groupByKey()
print('\n\n\n\n User history created')


ngramRDD = userHistoryRDD.flatMap(lambda line: userHistorytoNgram(line))
print('\n\n\n\n Ngrams created')

outputRDD = ngramRDD.reduceByKey(lambda a,b : a + b).map(lambda line: mapToOutputFormat(line))
print('\n\n\n\n Waiting to collect')
print('\n\n')
#print(outputRDD.collect())
#print('\n')


#outputRDD.saveAsTextFile(outFile)


#Write to single .txt file to match CCM input
outputList = outputRDD.collect()

if os.path.isfile(outFile):
    os.remove(outFile)    
out = open(outFile,'a')

for line in outputList:
	out.write(line)




print('\n\n\n\n')
print("#####################################################")
print("#####################################################")




