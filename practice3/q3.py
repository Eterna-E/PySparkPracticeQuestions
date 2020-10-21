# 3、创建一个元素为 1-5 的RDD，
# 运用 flatMap创建一个新的 RDD，
# 新的 RDD 为原 RDD 每个元素的 平方和三次方 
# 来组成 1,1,4,8,9,27..

from pyspark import SparkContext

sc = SparkContext("local", "Simple App")
numList = [x for x in range(1,6)]
rdd = sc.parallelize(numList)

data3 = rdd.flatMap(lambda x: (x**2,x**3)).collect()
print(data3)

# ok