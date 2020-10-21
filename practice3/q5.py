# 5、创建一个 RDD数据为Array(1, 3, 4, 20, 4, 5, 8)，
# 按照元素的奇偶性进行分组

from pyspark import SparkContext

sc = SparkContext("local", "Simple App")
numList = [1, 3, 4, 20, 4, 5, 8]
rdd = sc.parallelize(numList)

data5 = rdd.groupBy(lambda x: x%2).collect()
print(data5)

data5 = rdd.groupBy(lambda x: x%2).map(lambda x: (x[0],list(x[1]))).collect()
print(data5)

# ok