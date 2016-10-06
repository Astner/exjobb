#spark_settings.py

from pyspark import SparkContext
import os,time
import shutil


sc = SparkContext()

print('\n\n\n')
print(sc._conf.getAll())