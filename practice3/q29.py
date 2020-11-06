#  /**
#   * 29、mysql创建一个数据库bigdata0407，在此数据库中创建一张表
#   * CREATE TABLE `user` (
#   * `id` int(11) NOT NULL AUTO_INCREMENT,
#   * `username` varchar(32) NOT NULL COMMENT '用户名称',
#   * `birthday` date DEFAULT NULL COMMENT '生日',
#   * `sex` char(1) DEFAULT NULL COMMENT '性别',
#   * `address` varchar(256) DEFAULT NULL COMMENT '地址',
#   * PRIMARY KEY (`id`)
#   * ) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;
#   * 数据如下：
#   * 依次是：姓名 生日 性别 省份
#   * 安荷 1998/2/7 女 江苏省
#   * 白秋 2000/3/7 女 天津市
#   * 雪莲 1998/6/7 女 湖北省
#   * 宾白 1999/7/3 男 河北省
#   * 宾实 2000/8/7 男 河北省
#   * 斌斌 1998/3/7 男 江苏省
#   * 请使用spark将以上数据写入mysql中，并读取出来。
#   */
#  val data29 = sc.textFile("input20200407/users.txt")
#  val driver = "com.mysql.jdbc.Driver"
#  val url = "jdbc:mysql://localhost:3306/bigdata0407"
#  val username = "root"
#  val password = "root"
# /**
#  * MySQL插入数据
#  */
#  data29.foreachPartition {
#    data =>
#      Class.forName(driver)
#      val connection = java.sql.DriverManager.getConnection(url, username, password)
#      val sql = "INSERT INTO `user` values (NULL,?,?,?,?)"
#      data.foreach {
#        tuples => {
#          val datas = tuples.split(" ")
#          val statement = connection.prepareStatement(sql)
#          statement.setString(1, datas(0))
#          statement.setString(2, datas(1))
#          statement.setString(3, datas(2))
#          statement.setString(4, datas(3))
#          statement.executeUpdate()
#          statement.close()
#        }
#      }
#      connection.close()
#  }
# /**
#  * MySQL查询数据
#   */
#  var sql = "select * from `user` where id between ? and ?"
#  val jdbcRDD = new JdbcRDD(sc,
#    () => {
#      Class.forName(driver)
#      java.sql.DriverManager.getConnection(url, username, password)
#    },
#    sql,
#    0,
#    44,
#    3,
#    result => {
#      println(s"id=${result.getInt(1)},username=${result.getString(2)}" +
#        s",birthday=${result.getDate(3)},sex=${result.getString(4)},address=${result.getString(5)}")
#    }
#  )
#  jdbcRDD.collect()

import pandas as pd
from pyspark import SparkContext
from pyspark.sql import SQLContext, Row

if __name__ == '__main__':
    # spark 初始化
    sc = SparkContext(master='local', appName='sql')
    spark = SQLContext(sc)
    # mysql 配置(需要修改)
    prop = {'user': 'root',
            'password': 'rootroot',
            'driver': 'com.mysql.cj.jdbc.Driver'}
    # database 地址(需要修改)
    url = 'jdbc:mysql://localhost:3306/testdb?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC'
    data29 = sc.textFile("mysql_insert.txt")
    data29result = data29.map(lambda x: x.split(' '))
    # print(data29result.collect())
    username = data29.map(lambda x: x.split(' ')).map(lambda x: x[0]).collect()
    print(username)
    data_id = [x+1 for x in range(len(username))]
    print(data_id)
    birthday = data29.map(lambda x: x.split(' ')).map(lambda x: x[1]).collect()
    print(birthday)
    sex = data29.map(lambda x: x.split(' ')).map(lambda x: x[2]).collect()
    print(sex)
    address = data29.map(lambda x: x.split(' ')).map(lambda x: x[3]).collect()
    print(address)

    # 创建spark DataFrame
    # 方式3：pandas dataFrame 转spark DataFrame
    # 安荷 1998/2/7 女 江苏省
    df = pd.DataFrame({'id': data_id, 'username': username,
                       'birthday': birthday, 'sex': sex, 'address': address})
    pd_df = spark.createDataFrame(df)

    # 写入数据库
    pd_df.write.jdbc(url=url, table='user', mode='overwrite', properties=prop)

    # 读取表
    data = spark.read.jdbc(url=url, table='user', properties=prop)
    # 打印data数据类型
    print(type(data))
    # 展示数据
    data.show()

# ok

# 參考：https://zhuanlan.zhihu.com/p/136777424
