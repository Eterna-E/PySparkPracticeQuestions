# 16、创建两个RDD，
# 分别为rdd1和rdd2数据分别为1 to 6和4 to 10，
# 计算 2 个 RDD 的笛卡尔积
#  val data16_1 = sc.makeRDD(1 to 6)
#  val data16_2 = sc.makeRDD(4 to 10)
#  val data16Result = data16_1.cartesian(data16_2)

from pyspark import SparkContext

sc = SparkContext("local", "Simple App")
rdd1 = sc.parallelize([x for x in range(1,7)])
rdd2 = sc.parallelize([x for x in range(4,11)])

data16Result = rdd1.cartesian(rdd2).collect()

print("rdd1:",rdd1.collect())
print("rdd2:",rdd2.collect())
print("rdd1和rdd2的笛卡兒積:", sorted(data16Result))

# ok