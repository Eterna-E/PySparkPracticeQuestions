# // 18、创建一个RDD数据为
# List(("female",1),("male",5),("female",5),("male",2))
# ，请计算出female和male的总数分别为多少
#  val data18 = sc.makeRDD(List(("female", 1), ("male", 5), ("female", 5), ("male", 2)))
#  val data18Result = data18.reduceByKey(_ + _)

from pyspark import SparkContext
from operator import add

sc = SparkContext("local", "Simple App")
rdd1 = sc.parallelize([("female", 1), ("male", 5), ("female", 5), ("male", 2)])

data18Result = rdd1.reduceByKey(add).collect()

print("rdd1:",rdd1.collect())
print("female和male的总数:", sorted(data18Result))

# ok