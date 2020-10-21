# 7、创建一个 RDD数据为1 to 10，请使用sample不放回抽样

from pyspark import SparkContext

sc = SparkContext("local", "Simple App")
numList = [x for x in range(1,11)]
rdd = sc.parallelize(numList)

data7 = rdd.sample(
    withReplacement=False, # 無放回抽樣
    fraction=0.5
).collect()
print(data7)

# 下方抽樣五次，參考：https://www.iteblog.com/archives/1395.html#sample

print('origin:',rdd.collect())
sampleList = [rdd.sample(withReplacement=False,fraction=0.5) for i in range(5)]
# print(sampleList)
for cnt,y in zip(range(len(sampleList)), sampleList):
    print('sample ' + str(cnt) +' :'+ str(y.collect()))

# withReplacement = True or False代表是否有放回。fraction = x, where x = .5，代表抽取百分比

# 參考 https://zhuanlan.zhihu.com/p/34901846
# 或 https://www.cnblogs.com/tianqizhi/p/12115707.html
# 搜尋sample

# ok