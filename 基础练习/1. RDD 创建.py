import os
os.environ['PYSPARK_PYTHON'] = r'C:\ProgramData\Anaconda3\envs\PySpark\python.exe'

from pyspark import SparkContext


def demo1():
    sc = SparkContext()
    data = sc.parallelize([('Amber', 22), ('Alfred', 23), ('Skye', 4), ('Albert', 12), ('Amber', 9)])
    print(data)
    return


def demo2():
    """
    RDD 是无 Schema(模式) 的数据结构.
    因此, 可以混合几乎所有数据结构.
    SparkContext().parallelize() 将 python 对象分布式存储,
    .collect() 方法则又将其读取到当前计算机内存中, 并转换为 Python 对象.
    :return:
    """
    sc = SparkContext()
    data_heterogeneous = sc.parallelize([('Ferrari', 'fast'), {'Porsche': 100000}, ['Spain', 'visited', 4504]])
    # .collect() 方法 (执行把该数据集送回驱动的操作), 可以访问对象中的数据, 和在 Python 中常做的一样.
    ret = data_heterogeneous.collect()
    print(ret)
    print(ret[1]['Porsche'])
    return


def demo3():
    """
    从本地读取文件.
    :return:
    """
    test_file_path = "file:///D:/Users/Administrator/PycharmProjects/PySpark/dataset/test.txt"
    sc = SparkContext("local", "first app")
    test_file = sc.textFile(test_file_path)
    content = test_file.collect()
    # 文件的每一行被当作是一个对象存储.
    print(content)

    # 文档中包含 a, b 字符的行的数量.
    num_a = test_file.filter(lambda s: 'a' in s).count()
    num_b = test_file.filter(lambda s: 'b' in s).count()

    print("Line with a: %i,lines with b: %i" % (num_a, num_b))
    return


if __name__ == '__main__':
    # demo1()
    # demo2()
    demo3()
