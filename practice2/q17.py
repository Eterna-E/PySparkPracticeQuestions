# 題目：
# // 17. 13班男生平均总成绩是多少？175.0
# //(李逵,190)
# //(林冲,160)
from pyspark import SparkContext

logFile = "test.txt"  # Should be some file on your system
sc = SparkContext("local", "Simple App")
logData = sc.textFile(logFile).cache()

numBs = logData.map(lambda s: s.split(' ')).filter(lambda x: x[0]=='13' and x[3] == '男').collect()
print(numBs)

numBs = logData.map(lambda s: s.split(' ')).filter(lambda x: x[0]=='13' and x[3] == '男').map(lambda x: (x[1],int(x[5]))).collect()
print(numBs)

numBs = logData.map(lambda s: s.split(' ')).filter(lambda x: x[0]=='13' and x[3] == '男').map(lambda x: (x[1],int(x[5]))).groupByKey().map(lambda x : (x[0], list(x[1]))).collect()
print(numBs)

numBs = logData.map(lambda s: s.split(' ')).filter(lambda x: x[0]=='13' and x[3] == '男').map(lambda x: (x[1],int(x[5]))).groupByKey().map(lambda x : (x[0], list(x[1]))).map(lambda x : (x[0], sum(x[1]))).collect()
print(numBs)

numBs = logData.map(lambda s: s.split(' ')).filter(lambda x: x[0]=='13' and x[3] == '男').map(lambda x: (x[1],int(x[5]))).groupByKey().map(lambda x : (x[0], list(x[1]))).map(lambda x : (x[0], sum(x[1]))).map(lambda x: x[1]).collect()
print(numBs)

numBs = logData.map(lambda s: s.split(' ')).filter(lambda x: x[0]=='13' and x[3] == '男').map(lambda x: (x[1],int(x[5]))).groupByKey().map(lambda x : (x[0], list(x[1]))).map(lambda x : (x[0], sum(x[1]))).map(lambda x: x[1]).mean()
print(numBs)