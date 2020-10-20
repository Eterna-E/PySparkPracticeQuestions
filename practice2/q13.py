# 題目：
# 13. 12班平均成绩是多少？ 60.0

from pyspark import SparkContext

logFile = "test.txt"  # Should be some file on your system
sc = SparkContext("local", "Simple App")
logData = sc.textFile(logFile).cache()

numBs = logData.map(lambda s: s.split(' ')).filter(lambda x: x[0]=='12').collect()
print(numBs)

numBs = logData.map(lambda s: s.split(' ')).filter(lambda x: x[0]=='12').map(lambda x: int(x[5])).collect()
print(numBs)

numBs = logData.map(lambda s: s.split(' ')).filter(lambda x: x[0]=='12').map(lambda x: int(x[5])).mean()
print(numBs)