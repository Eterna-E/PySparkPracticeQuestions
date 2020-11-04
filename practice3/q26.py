# 26、读写 objectFile 文件，
# 把 RDD 保存为objectFile，
# RDD数据为Array(("a", 1),("b", 2),("c", 3))，
# 并进行读取出来
#  val data26_1 = 
# sc.makeRDD(Array(("a", 1), ("b", 2), ("c", 3)))
#  data26_1.
# saveAsObjectFile
# ("output20200407/20200407_objectFile")
#  val data26_2 = 
# sc.objectFile
# ("output20200407/20200407_objectFile")

# 參考：https://spark.apache.org/docs/2.3.1/api/python/_modules/pyspark/rdd.html

from pyspark import SparkContext

sc = SparkContext("local", "Simple App")
dataList = [("a", 1), ("b", 2), ("c", 3)]
rdd1 = sc.parallelize(dataList)
# data26Result = rdd1.saveAsPickleFile("ObjectFile")

data26 = sc.pickleFile("ObjectFile").collect()

print(data26)