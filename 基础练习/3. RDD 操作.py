"""

"""
import os
os.environ['PYSPARK_PYTHON'] = r'C:\ProgramData\Anaconda3\envs\PySpark\python.exe'

import re
import numpy as np
from pyspark import SparkContext


def demo1():
    test_file_path = "file:///D:/Users/Administrator/PycharmProjects/PySpark/dataset/test.txt"
    sc = SparkContext("local", "first app")
    test_rdd = sc.textFile(test_file_path)

    file_content = test_rdd.collect()
    print(f"RDD content: {file_content}")

    def func(x):
        l = x.split(sep=' ')
        k, v = int(l[0]), l[1]
        return k, v
    map_rdd = test_rdd.map(func)
    data_map = map_rdd.collect()
    print(f"RDD mapped content: {data_map}")

    # 从 RDD 中获取前 num 个元素. 返回值为列表, 不是 RDD 对象
    rdd_take = test_rdd.take(num=2)
    print(f"rdd take: {rdd_take}")

    # 随机抽取样本.
    rdd_take_sample = map_rdd.takeSample(True, 3, 0)
    print(f"rdd take sample: {rdd_take_sample}")

    # reduce() 方法 (聚合). 在每个分区里, reduce 方法运行指定函数. 之后, 再将该聚合结果返回给最终的驱动程序节点.
    # 需要注意的是, reduce 传递的函数需要是关联的, 即元素顺序改变, 结果不变, 该函数还需要是交换的, 即操作符顺序改变, 结果不变.
    # 关联: (5+2)+3 = 5+(2+3), 交换: 5+2+3 = 3+2+5
    map_rdd_reduce = map_rdd.map(lambda x: x[0]).reduce(lambda x, y: x + y)
    print(f"map rdd reduce: {map_rdd_reduce}")

    # .count()
    map_rdd_count = map_rdd.count()
    print(f"map rdd count: {map_rdd_count}")

    map_rdd_count_by_value = map_rdd.countByValue()
    print(f"map rdd count by value: {map_rdd_count_by_value}")

    map_rdd_count_by_value = map_rdd.map(lambda x: x[1]).countByValue()
    print(f"map rdd count by value: {map_rdd_count_by_value}")
    return


def demo2():
    sc = SparkContext("local", "first app")
    data_key = sc.parallelize([('a', 4), ('b', 3), ('c', 2), ('a', 8), ('d', 2), ('b', 1), ('d', 3)], 4)
    rdd_reduce_by_key = data_key.reduceByKey(lambda x, y: x + y).collect()
    print(f"rdd reduce by key: {rdd_reduce_by_key}")

    rdd_count_by_key = data_key.countByKey(lambda x, y: x + y).collect()
    print(f"rdd count by key: {rdd_count_by_key}")
    return


def demo3():
    """
    将 RDD 保存为文本文件, 每个分区存为一个文本.
    保存的文件为 saveAsTextFile.txt (目录). 其中 RDD 对象按分区分别存储在 part-0000{0-4}
    saveAsTextFile.txt
        .part-00000.crc
        .part-00001.crc
        .part-00002.crc
        .part-00003.crc
        ._SUCCESS.crc
        part-00000
        part-00001
        part-00002
        part-00003
        _SUCCESS

    :return:
    """
    file_path = "file:///D:/Users/Administrator/PycharmProjects/PySpark/dataset/temp/saveAsTextFile.txt"
    sc = SparkContext("local", "first app")
    data_key = sc.parallelize([('a', 4), ('b', 3), ('c', 2), ('a', 8), ('d', 2), ('b', 1), ('d', 3)], 4)
    data_key.saveAsTextFile(file_path)

    data_rdd = sc.textFile(file_path)
    print(data_rdd.collect())
    return


def demo4():
    """
    foreach() 方法
    此方法对 RDD 里的每个元素, 用迭代的方式应用相同的函数, 和 map() 相比, foreach 方法按照一个接一个的方式,
    对每一条记录应用一个定义好的函数. 当你希望将数据保存到 PySpark 本身不支持的数据库时, 该方法很有用.

    这里, 我们用它来打印 (打印到 CLI) 存储在 RDD 中的所有记录.
    注意, 每次记录的顺序可能不同.
    """
    sc = SparkContext("local", "first app")
    data_key = sc.parallelize([('a', 4), ('b', 3), ('c', 2), ('a', 8), ('d', 2), ('b', 1), ('d', 3)], 4)

    def f(x):
        print(x)
        return
    data_key.foreach(f)
    return


if __name__ == '__main__':
    demo4()
