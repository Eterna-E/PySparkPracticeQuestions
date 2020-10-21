# 17、创建两个RDD，
# 分别为rdd1和rdd2数据分别为1 to 5和11 to 15，
# 对两个RDD拉链操作
#  val data17_1 = sc.makeRDD(1 to 5)
#  val data17_2 = sc.makeRDD(11 to 15)
#  val data17Result = data17_1.zip(data17_2)
# zip 可參考 https://www.iteblog.com/archives/1400.html#zip
# 或 http://lxw1234.com/archives/2015/07/350.htm

from pyspark import SparkContext

sc = SparkContext("local", "Simple App")
rdd1 = sc.parallelize([x for x in range(1,6)])
rdd2 = sc.parallelize([x for x in range(11,16)])

data17Result = rdd1.zip(rdd2).collect()

print("rdd1:",rdd1.collect())
print("rdd2:",rdd2.collect())
print("rdd1和rdd2的拉链操作:", sorted(data17Result))

# ok