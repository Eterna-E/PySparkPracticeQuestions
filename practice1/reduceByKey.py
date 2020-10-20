from pyspark import SparkContext
from operator import add
logFile = "test.txt"  # Should be some file on your system
sc = SparkContext("local", "Simple App")
logData = sc.textFile(logFile).cache()
numBs = logData.flatMap(lambda s: s.split(','))\
	.map(lambda b: (b,1))\
	.reduceByKey(add)\
	.collect()

# numBs = logData.flatMap(lambda  s: s.split(',')).collect()

print(numBs)