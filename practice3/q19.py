# // 19、创建一个有两个分区的 RDD数据为
# List(("a",3),("a",2),("c",4),
# ("b",3),("c",6),("c",8))，
# 取出每个分区相同key对应值的最大值，然后相加
#  /**
#   * (a,3),(a,2),(c,4)
#   * (b,3),(c,6),(c,8)
#   */
#  val data19 = 
# sc.makeRDD(List(("a", 3), ("a", 2), ("c", 4),
#  ("b", 3), ("c", 6), ("c", 8)), 2)
#  data19.glom().collect().foreach(x => 
# println(x.mkString(",")))
#  val data19Result = data19.aggregateByKey(0)
# (math.max(_, _), _ + _)

# 參考：https://blog.csdn.net/zhuzuwei/article/details/104446388

from pyspark import SparkContext

sc = SparkContext("local", "Simple App")
dataList = [("a", 3), ("a", 2), ("c", 4),("b", 3), ("c", 6), ("c", 8)]
rdd1 = sc.parallelize(dataList,2)
data19 = rdd1.glom().collect()
print(data19)
maxVal = (lambda x,y: max(x,y))
sumComb = (lambda x,y: x+y)
data19Result = rdd1.aggregateByKey(0,maxVal,sumComb).collect()

print("每个分区相同key对应值的最大值:", sorted(data19Result))

# ok