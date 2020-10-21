# 13、创建两个RDD，
# 分别为rdd1和rdd2数据分别为1 to 6和4 to 10，
# 求并集
#  val data13_1 = sc.makeRDD(1 to 6)
#  val data13_2 = sc.makeRDD(4 to 10)
#  val data13Result = data13_1.union(data13_2)
# 并集 == 台灣的聯集
# 若A和B是集合，
# 則A和B聯集就是包含所有A的元素和所有B的元素，
# 而沒有其他元素的集合。

from pyspark import SparkContext

sc = SparkContext("local", "Simple App")
rdd1 = sc.parallelize([x for x in range(1,7)])
rdd2 = sc.parallelize([x for x in range(4,11)])

data13Result = rdd1.union(rdd2).collect()
print(data13Result) # rdd1 和 rdd2 的聯集

# ok