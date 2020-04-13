# -*- coding:utf-8 -*-
import os
os.environ['PYSPARK_PYTHON'] = r'C:\ProgramData\Anaconda3\envs\PySpark\python.exe'

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, StringType


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
    df.printSchema()
    df.show(3)
    return


def demo2():
    """
    从 JSON 创建 DataFrame
    :return:
    """
    sc = SparkContext()
    stringJSONRDD = sc.parallelize((
        """{ 
            "id": "123",
            "name": "Katie",
            "age": 19,
            "eyeColor": "brown"
        }""",
        """{
            "id": "234",
            "name": "Michael",
            "age": 22,
            "eyeColor": "green"
        }""",
        """{
            "id": "345",
            "name": "Simone",
            "age": 23,
            "eyeColor": "blue"
        }"""))
    spark = SparkSession.builder.appName('first_app').getOrCreate()
    df_json = spark.read.json(stringJSONRDD)
    # 查看模式
    df_json.printSchema()
    df_json.show()

    df_json.createOrReplaceTempView("swimmers_json")
    ret = spark.sql("SELECT * FROM swimmers_json")
    ret.show()
    print(ret.collect())
    return


def demo3():
    """
    从 RDD 创建 DataFrame
    不能从 DataFrame 创建 DataFrame
    """
    sc = SparkContext()
    string_csv_rdd = sc.parallelize([(123, 'Katie', 19, 'brown'),
                                     (234, 'Michael', 22, 'green'),
                                     (345, 'Simone', 23, 'blue')])
    # 创建 schema 模式.
    schema = StructType([StructField("id", LongType(), True),
                         StructField("name", StringType(), True),
                         StructField("age", LongType(), True),
                         StructField("eyeColor", StringType(), True)])
    spark = SparkSession.builder.appName('first_app').getOrCreate()
    swimmers = spark.createDataFrame(string_csv_rdd, schema)
    swimmers.createOrReplaceGlobalTempView("swimmers")
    swimmers.printSchema()
    return


def demo4():
    """
    从 Python 对象创建 DataFrame
    """
    spark = SparkSession.builder.appName('first_app').getOrCreate()
    df = spark.createDataFrame(data=[(1, 144.5, 5.9, 33, 'M'),
                                     (2, 167.2, 5.4, 45, 'M'),
                                     (3, 124.1, 5.2, 23, 'F'),
                                     (4, 144.5, 5.9, 33, 'M'),
                                     (5, 133.2, 5.7, 54, 'F'),
                                     (3, 124.1, 5.2, 23, 'F'),
                                     (5, 129.2, 5.3, 42, 'M')],
                               schema=['id', 'weight', 'height', 'age', 'gender'])
    df.show()
    df.printSchema()

    return


if __name__ == '__main__':
    demo1()
    # demo2()
    # demo3()
