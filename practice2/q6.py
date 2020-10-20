# 題目：
# 6. 一共有多少个女生参加考试？
# ans:4
from pyspark import SparkContext

logFile = "test.txt"  # Should be some file on your system
sc = SparkContext("local", "Simple App")
logData = sc.textFile(logFile).cache()

numBs = logData.map(lambda s: s.split(' ')).filter(lambda x: x[3]=='女').collect()
print(numBs)

numBs = logData.map(lambda s: s.split(' ')).filter(lambda x: x[3]=='女').groupBy(lambda x:x[1]).collect()
print(numBs)

numBs = logData.map(lambda s: s.split(' ')).filter(lambda x: x[3]=='女').groupBy(lambda x:x[1]).count()
print(numBs)