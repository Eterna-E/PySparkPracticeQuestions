# 題目：
# // 21. 13班数学最高成绩是多少？80
# var max3 = data.map(x => x.split(" ")).filter(x => x(4).equals("math") && x(0).equals("13")).map(x => x(5).toInt).max()

from pyspark import SparkContext

logFile = "test.txt"  # Should be some file on your system
sc = SparkContext("local", "Simple App")
logData = sc.textFile(logFile).cache()

numBs = logData.map(lambda s: s.split(' ')).filter(lambda x: x[4]=='math' and x[0]=='13').collect()
print(numBs)

numBs = logData.map(lambda s: s.split(' ')).filter(lambda x: x[4]=='math' and x[0]=='13').map(lambda x: int(x[5])).collect()
print(numBs)

numBs = logData.map(lambda s: s.split(' ')).filter(lambda x: x[4]=='math' and x[0]=='13').map(lambda x: int(x[5])).max()
print(numBs)