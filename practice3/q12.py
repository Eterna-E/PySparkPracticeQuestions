# 12、创建一个 RDD数据为1,3,4,10,4,6,9,20,30,16,
# 请给RDD进行分别进行升序和降序排列
#  val data12 = sc.makeRDD(Array(1, 3, 4, 10, 4, 6, 9, 20, 30, 16))
#  val data12Result1 = data12.sortBy(x => x)
#  val data12Result2 = data12.sortBy(x => x, false)

from pyspark import SparkContext

sc = SparkContext("local", "Simple App")
numList = [1,3,4,10,4,6,9,20,30,16]
rdd = sc.parallelize(numList)

data12Result1 = rdd.sortBy(lambda x: x).collect()
print(data12Result1) # 升序排列

data12Result2 = rdd.sortBy(lambda x: x,False).collect()
print(data12Result2) # 降序排列

# ok