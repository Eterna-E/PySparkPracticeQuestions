# 2、创建一个10-20数组的RDD，使用mapPartitions将所有元素*2形成新的RDD

from pyspark import SparkContext

sc = SparkContext("local", "Simple App")
numList = [x for x in range(10,20)]
rdd = sc.parallelize(numList,len(numList))
def f(part):
    print ("====")
    for row in part:
        re = row*2
        yield re
    return re
data2 = rdd.mapPartitions(f).collect()
print(data2)

# ok