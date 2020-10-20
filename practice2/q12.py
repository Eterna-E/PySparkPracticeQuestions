# 題目：
# 12. 每个人平均成绩是多少？
# (王英,73)
# (杨春,70)
# (宋江,60)
# (李逵,63)
# (吴用,50)
# (林冲,53)
# val every_socre: RDD[(String, Any)] = data.map(x=>x.split(" ")).map(x=>(x(1),x(5).toInt)).groupByKey().map(t=>(t._1,t._2.sum /t._2.size))


from pyspark import SparkContext

logFile = "test.txt"  # Should be some file on your system
sc = SparkContext("local", "Simple App")
logData = sc.textFile(logFile).cache()

numBs = logData.map(lambda s: s.split(' ')).map(lambda x: (x[1],int(x[5]))).collect()
print(numBs)

numBs = logData.map(lambda s: s.split(' ')).map(lambda x: (x[1],int(x[5]))).groupByKey().map(lambda x : (x[0], list(x[1]))).collect()
print(numBs)

numBs = logData.map(lambda s: s.split(' ')).map(lambda x: (x[1],int(x[5]))).groupByKey().map(lambda x : (x[0], list(x[1]))).map(lambda x : (x[0], int(sum(x[1])/len(x[1])) )).collect()
print(numBs)