# 題目：
# // 19. 全校语文成绩最高分是多少？70
# var max1 = data.map(x => x.split(" ")).filter(x => x(4).equals("chinese")).map(x => x(5).toInt).max()

from pyspark import SparkContext

logFile = "test.txt"  # Should be some file on your system
sc = SparkContext("local", "Simple App")
logData = sc.textFile(logFile).cache()

numBs = logData.map(lambda s: s.split(' ')).filter(lambda x: x[4]=='chinese').collect()
print(numBs)

numBs = logData.map(lambda s: s.split(' ')).filter(lambda x: x[4]=='chinese').map(lambda x: int(x[5])).collect()
print(numBs)

numBs = logData.map(lambda s: s.split(' ')).filter(lambda x: x[4]=='chinese').map(lambda x: int(x[5])).max()
print(numBs)