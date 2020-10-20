# 題目：
# // 20. 12班语文成绩最低分是多少？50
# var max2 = data.map(x => x.split(" ")).filter(x => x(4).equals("chinese") && x(0).equals("12")).map(x => x(5).toInt).min()

from pyspark import SparkContext

logFile = "test.txt"  # Should be some file on your system
sc = SparkContext("local", "Simple App")
logData = sc.textFile(logFile).cache()

numBs = logData.map(lambda s: s.split(' ')).filter(lambda x: x[4]=='chinese' and x[0]=='12').collect()
print(numBs)

numBs = logData.map(lambda s: s.split(' ')).filter(lambda x: x[4]=='chinese' and x[0]=='12').map(lambda x: int(x[5])).collect()
print(numBs)

numBs = logData.map(lambda s: s.split(' ')).filter(lambda x: x[4]=='chinese' and x[0]=='12').map(lambda x: int(x[5])).min()
print(numBs)