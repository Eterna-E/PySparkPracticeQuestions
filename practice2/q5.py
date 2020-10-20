# 題目：
# 5. 一共有多个男生参加考试？
# ans:4
from pyspark import SparkContext

logFile = "test.txt"  # Should be some file on your system
sc = SparkContext("local", "Simple App")
logData = sc.textFile(logFile).cache()

numBs = logData.map(lambda s: s.split(' ')).filter(lambda x: x[3]=='男').collect()
print(numBs)

numBs = logData.map(lambda s: s.split(' ')).filter(lambda x: x[3]=='男').groupBy(lambda x:x[1]).collect()
print(numBs)

numBs = logData.map(lambda s: s.split(' ')).filter(lambda x: x[3]=='男').groupBy(lambda x:x[1]).count()
print(numBs)