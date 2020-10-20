# 題目：
# 4. 一共有多少个大于20岁的人参加考试？
# ans:2
from pyspark import SparkContext

logFile = "test.txt"  # Should be some file on your system
sc = SparkContext("local", "Simple App")
logData = sc.textFile(logFile).cache()

numBs = logData.map(lambda s: s.split(' ')).filter(lambda x: int(x[2])>20).collect()
print(numBs)

numBs = logData.map(lambda s: s.split(' ')).filter(lambda x: int(x[2])>20).groupBy(lambda x:x[1]).collect()
print(numBs)

numBs = logData.map(lambda s: s.split(' ')).filter(lambda x: int(x[2])>20).groupBy(lambda x:x[1]).count()
print(numBs)