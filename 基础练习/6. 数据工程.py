# -*- coding:utf-8 -*-
import os
os.environ['PYSPARK_PYTHON'] = r'C:\ProgramData\Anaconda3\envs\PySpark\python.exe'

from pyspark import SparkContext
from pyspark.sql import SparkSession
import pyspark.sql.functions as fn
import pyspark.sql.types as types


def demo1():
    """
    重复数据.
    1. 完全相同.
    2. id 不同, 其它内容相同.
    3. 内容不同, 但 id 重复.
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

    # 判断是否存在完全重复的数据.
    print(f'Count of rows: {df.count()}')
    print(f'Count of distinct rows: {df.distinct().count()}')
    # 移除重复行.
    df = df.drop_duplicates()
    df.show()

    # id 不属于数据, 判断是否存在只有 id 不同的重复数据.
    print(f'Count of rows: {df.count()}')
    count_without_id = df.select([c for c in df.columns if c != 'id']).distinct().count()
    print(f"Count of distinct ids: {count_without_id}")

    # 移除重复行. 用 subset 指定的子集作为标准.
    df = df.drop_duplicates(subset=[c for c in df.columns if c != 'id'])
    df.show()

    # 查询 df 中 id 及不重复的 id 的数量.
    df.agg(fn.count('id').alias('count'), fn.countDistinct('id').alias('distinct')).show()

    # 有两个 id 重复的不同数据. 为 df 增加一个新字段, 其值为自动递增的且唯一的 id 值.
    df.withColumn('new_id', fn.monotonically_increasing_id()).show()
    return


def demo2():
    """
    未观测数据(缺失值处理)
    1. 如果数据是离散布尔型, 可以将该字段当作是分类变量, 将缺失值定义为一个新的类别 Missing.
    2. 如果数据是分类的, 则简单地添加一个新的类别 Missing.
    3. 如果是连续值, 则可以填充任何的平均数, 中间值或者其他预定义值(如, 第一个四分位数或者第三个四分位数).
    :return:
    """
    spark = SparkSession.builder.appName('first_app').getOrCreate()
    df = spark.createDataFrame(data=[(1, 143.5, 5.6, 28, 'M', 100000),
                                     (2, 167.2, 5.4, 45, 'M', None),
                                     (3, None, 5.2, None, None, None),
                                     (4, 144.5, 5.9, 33, 'M', None),
                                     (5, 133.2, 5.7, 54, 'F', None),
                                     (6, 124.1, 5.2, None, 'F', None),
                                     (7, 129.2, 5.3, 42, 'M', 76000)],
                               schema=['id', 'weight', 'height', 'age', 'gender', 'income'])

    # print(df.rdd.collect())
    # 查看各数据有多少缺失值.
    ret = df.rdd.map(lambda row: (row['id'], sum([c is None for c in row]))).collect()
    print(ret)

    # 查询指定 id 的数据.
    df.where('id == 3').show()
    df = df.select('*').where('id != 3')
    df.show()

    # 检查每一列中缺失值的占比.
    df.agg(*[(1 - (fn.count(c) / fn.count('*'))).alias(c + '_missing') for c in df.columns]).show()

    # 移除 income 字段
    # df = df.select([c for c in df.columns if c != 'income'])
    df = df.drop('income')
    df.show()

    # 移除有缺失值的行.
    df = df.dropna(thresh=3)
    df.show()

    means = df.agg(*[fn.mean(c).alias(c) for c in df.columns if c != 'gender'])
    means.show()

    means = means.toPandas().to_dict('record')[0]
    means['gender'] = 'missing'
    print(means)

    # 用 means 字典对各字段的缺失值进行填充.
    df = df.fillna(means)
    df.show()
    return


def demo3():
    """
    离群值 (异常数据)
    一般, 如果所有的值大致在 Q1-1.5IQR 和 Q3+1.5IQR 范围内, IQR 指四分位范围, 则可以认为没有离群值.
    IQR 定义为上分位 (upper quartile) 和下分位 (lower quartile) 之差, 也就是第 75% 分位与 25% 分位之差.
    """
    spark = SparkSession.builder.appName('first_app').getOrCreate()
    df = spark.createDataFrame(data=[(1, 143.5, 5.3, 28),
                                     (2, 154.2, 5.5, 45),
                                     (3, 342.3, 5.1, 99),
                                     (4, 144.5, 5.5, 33),
                                     (5, 133.2, 5.4, 54),
                                     (6, 124.1, 5.1, 21),
                                     (7, 129.2, 5.3, 42)],
                               schema=['id', 'weight', 'height', 'age'])
    weight_quantile = df.approxQuantile('weight', [0.25, 0.75], 0.05)
    q1, q3 = weight_quantile
    ior = q3 - q1
    lower_bound = q1 - 1.5 * ior
    upper_bound = q3 + 1.5 * ior
    df_outliers = df.select(*['id', ((df['weight'] < lower_bound) | (df['weight'] > upper_bound)).alias('weight_o'), 'height', 'age'])
    df_outliers.show()

    df_ = df.join(df_outliers, on='id')
    df_.show()

    df_ = df_.filter('weight_o').select('id', 'weight')
    df_.show()
    return


def demo4():
    """
    描述性统计
    http://packages.revolutionanalytics.com/datasets/ccFraud.csv

    要更好地理解分类列, 我们会利用 .groupBy() 方法计算这些列值的使用频率.
    :return:
    """
    sc = SparkContext(master="local", appName="first sc app")
    spark = SparkSession.builder.appName('first ss app').getOrCreate()
    file_path = 'file:///D:/Users/Administrator/PycharmProjects/PySpark/dataset/others/ccFraud/ccFraud.csv'
    fraud = sc.textFile(file_path)
    header = fraud.first()

    fraud = fraud.filter(lambda row: row != header).map(lambda row: [int(elem) for elem in row.split(',')])

    fields = [*[
        types.StructField(h[1:-1], types.IntegerType(), True) for h in header.split(',')
    ]]
    schema = types.StructType(fields)

    df = spark.createDataFrame(data=fraud, schema=schema)
    df.printSchema()

    numerical = ['balance', 'numTrans', 'numIntlTrans']
    desc = df.describe(numerical)
    desc.show()

    df.agg({'balance': 'skewness'}).show()
    return


def demo5():
    """
    相关性
    :return:
    """
    spark = SparkSession.builder.appName('first_app').getOrCreate()
    file = 'file:///D:/Users/Administrator/PycharmProjects/PySpark/dataset/others/ccFraud/ccFraud.csv'
    df = spark.read.csv(file, header=True, inferSchema=True)
    df.show(3)

    # Pearson 相关系数.
    ret = df.corr('balance', 'numTrans')
    print(ret)

    # 创建一个特征相关性矩阵
    numerical = ['balance', 'numTrans', 'numIntlTrans']



    return


if __name__ == '__main__':
    # demo1()
    # demo2()
    # demo3()
    # demo4()
    demo5()
