# 24、保存一个 SequenceFile 文件，
# 使用spark创建一个RDD数据为
# Array(("a", 1),("b", 2),("c", 3))，
# 保存为SequenceFile格式的文件到hdfs上
#  val data24 = 
# sc.makeRDD(Array(("a", 1), ("b", 2), ("c", 3)))
#  data24.saveAsSequenceFile
# ("hdfs://mycluster:8020/20200407_SequenceFile")

# 參考：https://stackoverflow.com/questions/34491579/saving-rdd-as-sequence-file-in-pyspark

from pyspark import SparkContext

sc = SparkContext("local", "Simple App")
dataList = [("a", 1),("b", 2),("c", 3)]
rdd1 = sc.parallelize(dataList)

data24Result = rdd1.saveAsSequenceFile("testSeq")

# ok