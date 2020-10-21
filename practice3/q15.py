# 15、创建两个RDD，
# 分别为rdd1和rdd2数据分别为1 to 6和4 to 10，
# 计算交集
#  val data15_1 = sc.makeRDD(1 to 6)
#  val data15_2 = sc.makeRDD(4 to 10)
#  val data15Result_1 = data15_1.intersection(data15_2)

# 兩個集合A和B的交集是含有所有既屬於A又屬於B的元素，
# 而沒有其他元素的集合。

from pyspark import SparkContext

sc = SparkContext("local", "Simple App")
rdd1 = sc.parallelize([x for x in range(1,7)])
rdd2 = sc.parallelize([x for x in range(4,11)])

data15Result = rdd1.intersection(rdd2).collect()

print("rdd1:",rdd1.collect())
print("rdd2:",rdd2.collect())
print("rdd1和rdd2的交集:", sorted(data15Result))

# ok