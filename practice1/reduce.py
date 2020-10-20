from pyspark import SparkContext
import re
a=[]
logFile = "test.txt"  # Should be some file on your system
sc = SparkContext("local", "Simple App")
logData = sc.textFile(logFile).cache()
numBs = logData.flatMap(lambda line: line.split(',')).map(lambda w:w[0])\
	.reduce(lambda a, b:a + b)
print(numBs)