from pyspark import SparkContext

logFile = "test.txt"
sc = SparkContext("local","Simple App")
logData = sc.textFile(logFile).cache()

numBs = logData.flatMap(lambda s: s.split(',')).collect()
print(numBs)
for i in numBs:
    print(i)