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

    # 创建spark DataFrame
    # 方式1：list转spark DataFrame
    l = [(1, 12), (2, 22)]
    # 创建并指定列名
    list_df = spark.createDataFrame(l, schema=['id', 'value']) 
    
    # 方式2：rdd转spark DataFrame
    rdd = sc.parallelize(l)  # rdd
    col_names = Row('id', 'value')  # 列名
    tmp = rdd.map(lambda x: col_names(*x))  # 设置列名
    rdd_df = spark.createDataFrame(tmp)  
    
    # 方式3：pandas dataFrame 转spark DataFrame
    df = pd.DataFrame({'id': [1, 2], 'value': [12, 22]})
    pd_df = spark.createDataFrame(df)

    # 写入数据库
    pd_df.write.jdbc(url=url, table='new', mode='append', properties=prop)
    # 关闭spark会话
    sc.stop()