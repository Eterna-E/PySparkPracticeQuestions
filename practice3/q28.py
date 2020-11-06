# /**
#   * 28、使用Spark广播变量
#   * 用户表：
#   * id name age gender(0|1)
#   * 001,刘向前,18,0
#   * 002,冯  剑,28,1
#   * 003,李志杰,38,0
#   * 004,郭  鹏,48,1
#   * 要求，输出用户信息，gender必须为男或者女，不能为0,1
#   * 使用广播变量把Map("0" -> "女", "1" -> "男")设置为广播变量，
#      最终输出格式为
#   * 001,刘向前,18,女
#   * 003,李志杰,38,女
#   * 002,冯  剑,28,男
#   * 004,郭  鹏,48,男
#   */
#  val data28 = sc.textFile("input20200407/user.txt")
#  val sex = sc.broadcast(Map("0" -> "女", "1" -> "男"))
#  data28.foreach { x => var datas = x.split(",");
#  println(datas(0) + "," + datas(1) + "," + datas(2) + "," + sex.value(datas(3))) }

# 參考：https://sparkbyexamples.com/pyspark/pyspark-broadcast-variables/

from pyspark import SparkContext

sc = SparkContext("local", "Simple App")
data28 = sc.textFile("user.txt")
# print(data28.collect())
sex = sc.broadcast({"0": "女", "1": "男"})

data28result = data28.map(lambda x: x.split(','))\
    .map(lambda x: x[0] + "," + x[1] + "," + x[2] + "," + sex.value[x[3]])
# print(data28result.collect())

for data in data28result.collect():
    print(data)

# ok
