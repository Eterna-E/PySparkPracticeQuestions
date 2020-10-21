# 14、创建两个RDD，
# 分别为rdd1和rdd2数据分别为1 to 6和4 to 10，
# 计算差集，两个都算
#  val data14_1 = sc.makeRDD(1 to 6)
#  val data14_2 = sc.makeRDD(4 to 10)
#  val data14Result_1 = data14_1.subtract(data14_2)
#  val data14Result_2 = data14_2.subtract(data14_1)

# 差集：若A和B是集合，
# 則A在B中的相對差集（簡稱差集）
# 是由所有屬於B但不屬於A的元素組成的集合。

from pyspark import SparkContext

sc = SparkContext("local", "Simple App")
rdd1 = sc.parallelize([x for x in range(1,7)])
rdd2 = sc.parallelize([x for x in range(4,11)])

data14Result_1 = rdd1.subtract(rdd2).collect()
data14Result_2 = rdd2.subtract(rdd1).collect()

print("rdd1:",rdd1.collect())
print("rdd2:",rdd2.collect())
print("差集rdd1\\rdd2:", sorted(data14Result_1))
print("差集rdd1\\rdd2:", sorted(data14Result_2))

# ok