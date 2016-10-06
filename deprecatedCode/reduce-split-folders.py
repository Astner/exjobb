#reduce-split-folders.py


from pyspark import SparkContext
import os,time
import shutil


sc =SparkContext()

print("#####################################################")
print("#####################################################")
print('\n\n\n\n')

print(sc.version)
start_time = time.time()


inputFolder = 'yahoo/data/ngram/split_dev_1k'


#outFile = 'yahoo/data/ngram/tempNRAM_3_users.txt'

print("#####################################################")


#if os.path.isdir(outFolder):
#	#os.rmdir(outFolder)
#	shutil.rmtree(outFolder)
#os.mkdir(outFolder) 


i = 1
# Vi har ca 100 folders att mergea 

#List all folders in the folder & make sure they end in .txt
for folder in os.listdir(inputFolder):
	#if file.endswith(".txt"):
	#	print(file)
	

	fullPath = inputFolder +'/'+folder
	if os.path.isdir(fullPath):
		print(folder)

		#Ineach folder, get each part-XXX item

		for file in os.listdir(fullPath):
			if file.startswith('part-'):

				print(file)

