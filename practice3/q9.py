# 9、创建一个 RDD数据为Array(10,10,2,5,3,5,3,6,9,1),
# 对 RDD 中元素执行去重操作
# val data9 = sc.makeRDD(Array(10, 10, 2, 5, 3, 5, 3, 6, 9, 1))
# val data9Result = data9.distinct()

from pyspark import SparkContext

sc = SparkContext("local", "Simple App")
numList = [10,10,2,5,3,5,3,6,9,1]
rdd = sc.parallelize(numList)

data9 = rdd.distinct().collect()
print(data9) # 未排序
data9 = sorted(data9) # 排序
print(data9)

# ok