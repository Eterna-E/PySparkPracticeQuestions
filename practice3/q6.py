# 6、创建一个 RDD（由字符串组成）
# Array("xiaoli", "laoli", "laowang", "xiaocang", "xiaojing", "xiaokong")，
# 过滤出一个新 RDD（包含“xiao”子串）

from pyspark import SparkContext

sc = SparkContext("local", "Simple App")
strList = ["xiaoli", "laoli", "laowang", "xiaocang", "xiaojing", "xiaokong"]
rdd = sc.parallelize(strList)

data6 = rdd.filter(lambda x: "xiao" in x).collect()
print(data6)

# ok