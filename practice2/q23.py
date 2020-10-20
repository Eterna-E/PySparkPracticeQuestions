# 題目：
# // 23. 总成绩大于150分，且数学大于等于70，且年龄大于等于19岁的学生的平均成绩是多少？
# // 过滤出总分大于150的,并求出平均成绩    (13,李逵,男,(60,1))               (13,李逵,男,(190,3))             总成绩大于150                (13,李逵,男,63)
# val com1: RDD[(String, Int)] = complex1.map(x=>(x._1,(x._2,1))).reduceByKey((a, b)=>(a._1+b._1,a._2+b._2)).filter(a=>(a._2._1>150)).map(t=>(t._1,t._2._1/t._2._2))


from pyspark import SparkContext

logFile = "test.txt"  # Should be some file on your system
sc = SparkContext("local", "Simple App")
logData = sc.textFile(logFile).cache()

# data1 = logData.map(lambda s: s.split(' '))\
#         .map(lambda x: (x[0]+','+x[1]+','+x[3],int(x[5])))\
#         .collect()
# print(data1)

# data1 = logData.map(lambda s: s.split(' '))\
#         .map(lambda x: (x[0]+','+x[1]+','+x[3],int(x[5])))\
#         .map(lambda x: (x[0],(x[1],1)))\
#         .collect()
# print(data1)

# data1 = logData.map(lambda s: s.split(' '))\
#         .map(lambda x: (x[0]+','+x[1]+','+x[3],int(x[5])))\
#         .map(lambda x: (x[0],(x[1],1)))\
#         .reduceByKey(lambda a,b: (a[0]+b[0],a[1]+b[1]))\
#         .collect()
# print(data1)

# data1 = logData.map(lambda s: s.split(' '))\
#         .map(lambda x: (x[0]+','+x[1]+','+x[3],int(x[5])))\
#         .map(lambda x: (x[0],(x[1],1)))\
#         .reduceByKey(lambda a,b: (a[0]+b[0],a[1]+b[1]))\
#         .filter(lambda x: x[1][0]>150)\
#         .collect()
# print(data1)

data1 = logData.map(lambda s: s.split(' '))\
        .map(lambda x: (x[0]+','+x[1]+','+x[3],int(x[5])))\
        .map(lambda x: (x[0],(x[1],1)))\
        .reduceByKey(lambda a,b: (a[0]+b[0],a[1]+b[1]))\
        .filter(lambda x: x[1][0]>150)\
        .map(lambda x: (x[0], int( x[1][0]/x[1][1] )))\
        .collect()
print(data1) # 过滤出总分大于150的,并求出平均成绩

# //过滤出 数学大于等于70，且年龄大于等于19岁的学生                filter方法返回一个boolean值 【数学成绩大于70并且年龄>=19】                                       为了将最后的数据集与com1做一个join,这里需要对返回值构造成com1格式的数据
# val com2: RDD[(String, Int)] = 
# complex2.filter(a=>{val line = a._1.split(",");
# line(4).equals("math") &&
#  a._2>=70 && line(2).toInt>=19})
# .map(a=>{val line2 = a._1.split(",");(line2(0)+","+line2(1)+","+line2(3),a._2.toInt)})
# //(12,杨春,女 , 70)
# //(13,王英,女 , 80)

data2 = logData.map(lambda s: s.split(' '))\
        .filter(lambda x: int(x[2])>=19 and x[4]=='math' and int(x[5])>=70)\
        .map(lambda x: (x[0]+','+x[1]+','+x[3] , int(x[5])))\
        .collect()
print(data2)

data1 = logData.map(lambda s: s.split(' '))\
        .map(lambda x: (x[0]+','+x[1]+','+x[3],int(x[5])))\
        .map(lambda x: (x[0],(x[1],1)))\
        .reduceByKey(lambda a,b: (a[0]+b[0],a[1]+b[1]))\
        .filter(lambda x: x[1][0]>150)\
        .map(lambda x: (x[0], int( x[1][0]/x[1][1] )))

data2 = logData.map(lambda s: s.split(' '))\
        .filter(lambda x: int(x[2])>=19 and x[4]=='math' and int(x[5])>=70)\
        .map(lambda x: (x[0]+','+x[1]+','+x[3] , int(x[5])))

print(data1.join(data2).collect())

# // 使用join函数聚合相同key组成的value元组
# // 再使用map函数格式化元素
# val result = com1.join(com2).map(a =>(a._1,a._2._1))
# //(12,杨春,女,70)
# //(13,王英,女,73)

print(data1.join(data2).map(lambda x: (x[0],x[1][0])).collect())