from pyspark import SparkContext
from pyspark.sql import SQLContext

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
    
    # 读取表
    data = spark.read.jdbc(url=url, table='user', properties=prop)
    # 打印data数据类型
    print(type(data))
    # 展示数据
    data.show()
    # 关闭spark会话
    sc.stop()