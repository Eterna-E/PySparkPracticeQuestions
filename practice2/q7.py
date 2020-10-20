# 題目：
# 7. 12班有多少人参加考试？
# ans:3
from pyspark import SparkContext

logFile = "test.txt"  # Should be some file on your system
sc = SparkContext("local", "Simple App")
logData = sc.textFile(logFile).cache()

numBs = logData.map(lambda s: s.split(' ')).filter(lambda x: x[0]=='12').collect()
print(numBs)

numBs = logData.map(lambda s: s.split(' ')).filter(lambda x: x[0]=='12').groupBy(lambda x:x[1]).collect()
print(numBs)

numBs = logData.map(lambda s: s.split(' ')).filter(lambda x: x[0]=='12').groupBy(lambda x:x[1]).count()
print(numBs)