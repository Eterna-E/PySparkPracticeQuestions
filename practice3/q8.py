# 8、创建一个 RDD数据为1 to 10，请使用sample放回抽样
# val data8 = sc.makeRDD(1 to 10)
# val data8Result = data8.sample(true, 0.5, 1)

from pyspark import SparkContext

sc = SparkContext("local", "Simple App")
numList = [x for x in range(1,11)]
rdd = sc.parallelize(numList)

data8 = rdd.sample(
    withReplacement=True, # 放回抽樣
    fraction=0.5
).collect()
print(data8)

# 下方抽樣五次，參考：https://www.iteblog.com/archives/1395.html#sample

print('origin:',rdd.collect())
sampleList = [rdd.sample(withReplacement=True,fraction=0.5) for i in range(5)]
# print(sampleList)
for cnt,y in zip(range(len(sampleList)), sampleList):
    print('sample ' + str(cnt) +' :'+ str(y.collect()))

# ok