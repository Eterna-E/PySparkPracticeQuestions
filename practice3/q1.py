# 1、创建一个1-10数组的RDD，将所有元素*2形成新的RDD

from pyspark import SparkContext

sc = SparkContext("local", "Simple App")
logData = sc.parallelize([x for x in range(10)]).cache()

data1 = logData.map(lambda x: x*2).collect()
print(data1)

# ok