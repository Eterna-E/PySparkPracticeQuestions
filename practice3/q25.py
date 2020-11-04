# 25、读取24题的SequenceFile 文件并输出
#  val data25: RDD[(String,Int)] = 
# sc.sequenceFile[String,Int]
# ("hdfs://mycluster:8020/20200407_SequenceFile/part-00000")

# 參考：https://blog.csdn.net/appleyuchi/article/details/81133270

from pyspark import SparkContext

sc = SparkContext("local", "Simple App")

data25Result = sc.sequenceFile("testSeq")

print(data25Result.values().collect())

# ok