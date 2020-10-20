# 題目：
# 10. 数学科目的平均成绩是多少？

from pyspark import SparkContext

logFile = "test.txt"  # Should be some file on your system
sc = SparkContext("local", "Simple App")
logData = sc.textFile(logFile).cache()

numBs = logData.map(lambda s: s.split(' ')).filter(lambda x: x[4]=='math').collect()
print(numBs)

numBs = logData.map(lambda s: s.split(' ')).filter(lambda x: x[4]=='math').map(lambda x: int(x[5])).collect()
print(numBs)

numBs = logData.map(lambda s: s.split(' ')).filter(lambda x: x[4]=='math').map(lambda x: int(x[5])).mean()
print(numBs)