# 4、创建一个 4 个分区的 RDD数据为
# Array(10,20,30,40,50,60)，
# 使用glom将每个分区的数据放到一个数组

from pyspark import SparkContext

sc = SparkContext("local", "Simple App")
numList = [x for x in range(10,70,10)]
rdd = sc.parallelize(numList).repartition(2)
print(rdd.collect())
print(rdd.getNumPartitions()) # partition size
data4 = rdd.glom().collect()
print(data4)

# ok