# -*- coding:utf-8 -*-
import os
os.environ['PYSPARK_PYTHON'] = r'C:\ProgramData\Anaconda3\envs\PySpark\python.exe'

from pyspark.sql import SparkSession
import pyspark.sql.functions as fn


def demo1():
    """
    从 csv 创建 DataFrame

    参考链接:
    https://www.jianshu.com/p/f48fc96620f3

    输出结果:
    DataFrame[PassengerId: int, Survived: int, Pclass: int, Name: string, Sex: string, Age: double, SibSp: int, Parch: int, Ticket: string, Fare: double, Cabin: string, Embarked: string]
    """
    spark = SparkSession.builder.appName('first_app').getOrCreate()
    file = 'file:///D:/Users/Administrator/PycharmProjects/PySpark/dataset/others/titanic/train.csv'
    df = spark.read.csv(file, header=True, inferSchema=True)
    print(df)
    df.show(3)

    print(f"df.dtypes 字段类型: {df.dtypes}")
    print(f"df.columns 字段名: {df.columns}")
    print(f"df.count() 字段名: {df.count()}")

    # 字段重命名 pandas: df.rename(columns={'Sex':sex})
    df = df.withColumnRenamed('Age', 'age').withColumnRenamed('Sex', 'sex')
    print(f"df.columns 字段名 after rename: {df.columns}")

    df.select('Age').show(2)
    df.select('Name', 'Age').show(2)

    df.select('Name', 'Age').filter(df['Age'] > 70).show()
    df.filter(df['Age'] > 30).filter(df['Sex'] == 'male').select('Name', 'Sex', 'Age').show()
    df.filter("Name like '%Mrs%'").show()

    # 首先 dataframe 注册为临时表，然后执行SQL查询
    print(f"SQL query: ")
    df.createOrReplaceTempView('df_sql')
    spark.sql('select Name,Age,Sex from df_sql where Age>30 and Age<35').show()

    df.drop('PassengerId').show()

    # 增加一列
    df.withColumn('id', fn.lit(0)).show()

    # pandas 排序
    # df.sort_values()
    # 排序 ascending=False 倒序
    df.sort('age', ascending=False).show()
    # 多列排序
    df.sort('age', 'fare', ascending=False).show()
    # 混合排序 age 字段倒序, fare 字段正序
    df.sort(df['Age'].desc(), df['Fare'].asc()).show()
    # orderBy 也是排序, 返回的 Row 对象列表
    df.orderBy('age', 'fare').take(3)

    # 查看是否有重复项
    print('去重前行数', df.count())
    print('去重后行数', df.distinct().count())
    # 去除重复行,同 pandas
    df.drop_duplicates()

    # 查看各字段缺失率
    df.agg(*[(1 - (fn.count(c) / fn.count('*'))).alias(c) for c in df.columns]).show()
    # 其中 age，Cabin，Embarked 字段有缺失

    # Cabin 缺失率较大删除该字段
    df = df.drop('cabin')
    # age 字段缺失率 20%, 填充均值
    agg_mean = round(df.select(fn.mean('age')).collect()[0][0], 0)
    # Embarked 缺失率较少，填充众数
    # 查看 Embarked 字段各分类总计
    df.groupby('embarked').count().sort('count', ascending=False).show()
    df = df.fillna({'age': agg_mean, 'embarked': 'S'})
    # 去除缺失值大于阈值的行
    # df.drop(thresh=)

    # 姓名为字符型, 从字符变量中提取有效信息
    str1 = df.select(fn.split('name', ',')[1].alias('name'))
    str2 = str1.select(fn.split('name', '. ')[0].alias('name'))
    str2.show()
    return


def demo2():
    """
    数据下载地址:
    https://databricks.com/try-databricks
    https://github.com/drabastomek/learningPySpark
    """
    spark = SparkSession.builder.appName('first_app').getOrCreate()
    file = 'file:///D:/Users/Administrator/PycharmProjects/PySpark/dataset/others/flight-data/departuredelays.csv'
    departure_delays = spark.read.csv(file, header=True, inferSchema=True, sep=',')
    departure_delays.show(3)
    file = 'file:///D:/Users/Administrator/PycharmProjects/PySpark/dataset/others/flight-data/airport-codes-na.txt'
    airport_codes_na = spark.read.csv(file, header=True, inferSchema=True, sep='\t')
    airport_codes_na.show(3)

    # 注册两个临时表, 用于 SQL 查询.
    departure_delays.createOrReplaceTempView('departure_delays')
    airport_codes_na.createOrReplaceTempView('airport_codes_na')

    spark.sql("""
        SELECT a.City,
        f.origin,
        SUM(f.delay) as Delays
        FROM departure_delays f
        JOIN airport_codes_na a 
        on a.IATA = f.origin
        WHERE a.State = 'WA'
        GROUP BY a.City, f.origin
        ORDER BY SUM(f.delay) DESC
        """).show()
    return


if __name__ == '__main__':
    # demo1()
    demo2()
