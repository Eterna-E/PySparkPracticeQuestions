from pyspark import SparkContext

logFile = "test.txt"
sc = SparkContext("local","Simple App")
logData = sc.textFile(logFile).cache()

numBs = logData.flatMap(lambda s: s.split(',')).filter(lambda line: "123" in line or "456" in line).collect()
print(numBs)
for i in numBs:
    print(i)