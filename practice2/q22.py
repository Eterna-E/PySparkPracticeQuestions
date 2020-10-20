# 題目：
# // 22. 总成绩大于150分的12班的女生有几个？1
# //(杨春,210)
# val count12_gt150girl: Long = data.map(x=>x.split(" ")).filter(x=>x(0).equals("12") && x(3).equals("女")).map(x=>(x(1),x(5).toInt)).groupByKey().map(t=>(t._1,t._2.sum)).filter(x=>x._2>150).count()

from pyspark import SparkContext

logFile = "test.txt"  # Should be some file on your system
sc = SparkContext("local", "Simple App")
logData = sc.textFile(logFile).cache()

numBs = logData.map(lambda s: s.split(' ')).filter(lambda x: x[0]=='12' and x[3] == '女').map(lambda x: (x[1],int(x[5]))).groupByKey().map(lambda x : (x[0], list(x[1]))).collect()
print(numBs)

numBs = logData.map(lambda s: s.split(' ')).filter(lambda x: x[0]=='12' and x[3] == '女').map(lambda x: (x[1],int(x[5]))).groupByKey().map(lambda x : (x[0], list(x[1]))).map(lambda x : (x[0], sum(x[1]))).filter(lambda x: x[1]>150).collect()
print(numBs)
