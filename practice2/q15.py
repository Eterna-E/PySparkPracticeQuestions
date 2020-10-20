# 題目：
# // 15. 12班女生平均总成绩是多少？210.0
# // (杨春,210)
from pyspark import SparkContext

logFile = "test.txt"  # Should be some file on your system
sc = SparkContext("local", "Simple App")
logData = sc.textFile(logFile).cache()

numBs = logData.map(lambda s: s.split(' ')).filter(lambda x: x[0]=='12' and x[3] == '女').map(lambda x: (x[1],int(x[5]))).groupByKey().map(lambda x : (x[0], list(x[1]))).map(lambda x : (x[0], sum(x[1]))).collect()
print(numBs)

numBs = logData.map(lambda s: s.split(' ')).filter(lambda x: x[0]=='12' and x[3] == '女').map(lambda x: (x[1],int(x[5]))).groupByKey().map(lambda x : (x[0], list(x[1]))).map(lambda x : (x[0], sum(x[1]))).map(lambda x: x[1]).collect()
print(numBs)

numBs = logData.map(lambda s: s.split(' ')).filter(lambda x: x[0]=='12' and x[3] == '女').map(lambda x: (x[1],int(x[5]))).groupByKey().map(lambda x : (x[0], list(x[1]))).map(lambda x : (x[0], sum(x[1]))).map(lambda x: x[1]).mean()
print(numBs)