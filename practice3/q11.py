# 11、创建一个分区数为5的 RDD，数据为0 to 100，
# 之后使用repartition再重新减少分区的数量至 3
#  val data11 = sc.makeRDD(0 to 100, 5)
#  val data11Result = data11.repartition(3)

from pyspark import SparkContext

sc = SparkContext("local", "Simple App")
numList = [x for x in range(100)]
rdd = sc.parallelize(numList,5)

data11 = rdd.glom().collect()
print(data11)

data11Result = rdd.repartition(3).glom().collect()
print(data11Result)

# ok