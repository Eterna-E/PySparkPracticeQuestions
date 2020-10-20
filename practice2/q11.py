# 題目：
# 11. 英语科目的平均成绩是多少？63.3333

from pyspark import SparkContext

logFile = "test.txt"  # Should be some file on your system
sc = SparkContext("local", "Simple App")
logData = sc.textFile(logFile).cache()

numBs = logData.map(lambda s: s.split(' ')).filter(lambda x: x[4]=='english').collect()
print(numBs)

numBs = logData.map(lambda s: s.split(' ')).filter(lambda x: x[4]=='english').map(lambda x: int(x[5])).collect()
print(numBs)

numBs = logData.map(lambda s: s.split(' ')).filter(lambda x: x[4]=='english').map(lambda x: int(x[5])).mean()
print(numBs)