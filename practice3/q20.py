# 20、创建一个有两个分区的 pairRDD数据为
# Array(("a", 88), ("b", 95), ("a", 91), 
# ("b", 93), ("a", 95), ("b", 98))，
# 根据 key 计算每种 key 的value的平均值
#  val data20 = sc.makeRDD(Array(("a", 88), ("b", 95), ("a", 91),
#  ("b", 93), ("a", 95), ("b", 98)))
#  val data20Result = data20.groupByKey()
# .map(x => x._1 -> x._2.sum / x._2.size)
#  //或val data20Result = data20
# .map(x => (x._1, (x._2, 1)))
# .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
# .map(x => (x._1, x._2._1 / x._2._2))

from pyspark import SparkContext

sc = SparkContext("local", "Simple App")
dataList = [("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98)]
rdd1 = sc.parallelize(dataList)

data20Result = rdd1.groupByKey().mapValues(list)\
    .map(lambda x: (x[0],format( sum(x[1])/len(x[1]), '.2f'))).collect()
print("rdd:",rdd1.collect())
print("计算每种 key 的value的平均值:", sorted(data20Result))

# ok