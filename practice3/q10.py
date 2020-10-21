# 10、创建一个分区数为5的 RDD，数据为0 to 100，
# 之后使用coalesce再重新减少分区的数量至 2
#  val data10 = sc.makeRDD(0 to 100, 5)
#  val data10Result = data10.coalesce(2)

from pyspark import SparkContext

sc = SparkContext("local", "Simple App")
numList = [x for x in range(100)]
rdd = sc.parallelize(numList,5)

data10 = rdd.glom().collect()
print(data10)

coalesceData10 = rdd.coalesce(2).glom().collect()
print(coalesceData10)

# ok