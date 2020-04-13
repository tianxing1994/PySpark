import os
os.environ['PYSPARK_PYTHON'] = r'C:\ProgramData\Anaconda3\envs\PySpark\python.exe'

import re
import numpy as np
from pyspark import SparkContext


def extract_information(row):
    """
    https://github.com/drabastomek/learningPySpark/blob/master/Chapter02/LearningPySpark_Chapter02.ipynb

    Input record schema
    schema: n-m (o) -- xxx
        n - position from
        m - position to
        o - number of characters
        xxx - description
    1. 1-19 (19) -- reserved positions
    2. 20 (1) -- resident status
    3. 21-60 (40) -- reserved positions
    4. 61-62 (2) -- education code (1989 revision)
    5. 63 (1) -- education code (2003 revision)
    6. 64 (1) -- education reporting flag
    7. 65-66 (2) -- month of death
    8. 67-68 (2) -- reserved positions
    9. 69 (1) -- sex
    10. 70 (1) -- age: 1-years, 2-months, 4-days, 5-hours, 6-minutes, 9-not stated
    11. 71-73 (3) -- number of units (years, months etc)
    12. 74 (1) -- age substitution flag (if the age reported in positions 70-74 is calculated using dates of birth and death)
    13. 75-76 (2) -- age recoded into 52 categories
    14. 77-78 (2) -- age recoded into 27 categories
    15. 79-80 (2) -- age recoded into 12 categories
    16. 81-82 (2) -- infant age recoded into 22 categories
    17. 83 (1) -- place of death
    18. 84 (1) -- marital status
    19. 85 (1) -- day of the week of death
    20. 86-101 (16) -- reserved positions
    21. 102-105 (4) -- current year
    22. 106 (1) -- injury at work
    23. 107 (1) -- manner of death
    24. 108 (1) -- manner of disposition
    25. 109 (1) -- autopsy
    26. 110-143 (34) -- reserved positions
    27. 144 (1) -- activity code
    28. 145 (1) -- place of injury
    29. 146-149 (4) -- ICD code
    30. 150-152 (3) -- 358 cause recode
    31. 153 (1) -- reserved position
    32. 154-156 (3) -- 113 cause recode
    33. 157-159 (3) -- 130 infant cause recode
    34. 160-161 (2) -- 39 cause recode
    35. 162 (1) -- reserved position
    36. 163-164 (2) -- number of entity-axis conditions
    37-56. 165-304 (140) -- list of up to 20 conditions
    57. 305-340 (36) -- reserved positions
    58. 341-342 (2) -- number of record axis conditions
    59. 343 (1) -- reserved position
    60-79. 344-443 (100) -- record axis conditions
    80. 444 (1) -- reserve position
    81. 445-446 (2) -- race
    82. 447 (1) -- bridged race flag
    83. 448 (1) -- race imputation flag
    84. 449 (1) -- race recode (3 categories)
    85. 450 (1) -- race recode (5 categories)
    86. 461-483 (33) -- reserved positions
    87. 484-486 (3) -- Hispanic origin
    88. 487 (1) -- reserved
    89. 488 (1) -- Hispanic origin/race recode
    """

    selected_indices = [
         2, 4, 5, 6, 7, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18,
         19, 21, 22, 23, 24, 25, 27, 28, 29, 30, 32, 33, 34,
         36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48,
         49, 50, 51, 52, 53, 54, 55, 56, 58, 60, 61, 62, 63,
         64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76,
         77, 78, 79, 81, 82, 83, 84, 85, 87, 89
    ]
    record_split = re.compile(
            r'([\s]{19})([0-9]{1})([\s]{40})([0-9\s]{2})([0-9\s]{1})([0-9]{1})([0-9]{2})' +
            r'([\s]{2})([FM]{1})([0-9]{1})([0-9]{3})([0-9\s]{1})([0-9]{2})([0-9]{2})' +
            r'([0-9]{2})([0-9\s]{2})([0-9]{1})([SMWDU]{1})([0-9]{1})([\s]{16})([0-9]{4})' +
            r'([YNU]{1})([0-9\s]{1})([BCOU]{1})([YNU]{1})([\s]{34})([0-9\s]{1})([0-9\s]{1})' +
            r'([A-Z0-9\s]{4})([0-9]{3})([\s]{1})([0-9\s]{3})([0-9\s]{3})([0-9\s]{2})([\s]{1})' +
            r'([0-9\s]{2})([A-Z0-9\s]{7})([A-Z0-9\s]{7})([A-Z0-9\s]{7})([A-Z0-9\s]{7})' +
            r'([A-Z0-9\s]{7})([A-Z0-9\s]{7})([A-Z0-9\s]{7})([A-Z0-9\s]{7})([A-Z0-9\s]{7})' +
            r'([A-Z0-9\s]{7})([A-Z0-9\s]{7})([A-Z0-9\s]{7})([A-Z0-9\s]{7})([A-Z0-9\s]{7})' +
            r'([A-Z0-9\s]{7})([A-Z0-9\s]{7})([A-Z0-9\s]{7})([A-Z0-9\s]{7})([A-Z0-9\s]{7})' +
            r'([A-Z0-9\s]{7})([\s]{36})([A-Z0-9\s]{2})([\s]{1})([A-Z0-9\s]{5})([A-Z0-9\s]{5})' +
            r'([A-Z0-9\s]{5})([A-Z0-9\s]{5})([A-Z0-9\s]{5})([A-Z0-9\s]{5})([A-Z0-9\s]{5})' +
            r'([A-Z0-9\s]{5})([A-Z0-9\s]{5})([A-Z0-9\s]{5})([A-Z0-9\s]{5})([A-Z0-9\s]{5})' +
            r'([A-Z0-9\s]{5})([A-Z0-9\s]{5})([A-Z0-9\s]{5})([A-Z0-9\s]{5})([A-Z0-9\s]{5})' +
            r'([A-Z0-9\s]{5})([A-Z0-9\s]{5})([A-Z0-9\s]{5})([\s]{1})([0-9\s]{2})([0-9\s]{1})' +
            r'([0-9\s]{1})([0-9\s]{1})([0-9\s]{1})([\s]{33})([0-9\s]{3})([0-9\s]{1})([0-9\s]{1})')
    try:
        rs = np.array(record_split.split(row))[selected_indices]
    except:
        rs = np.array(['-99'] * len(selected_indices))
    return rs


def demo1():
    """
    从本地读取文件来创建 RDD.
    :return:
    """
    test_file_path = "file:///D:/Users/Administrator/PycharmProjects/PySpark/dataset/test.txt"
    sc = SparkContext("local", "first app")
    test_rdd = sc.textFile(test_file_path)

    file_content = test_rdd.collect()
    print(f"RDD content: {file_content}")

    data_map = test_rdd.map(lambda x: str(x) + ' miyuki').collect()
    print(f"RDD mapped content: {data_map}")

    data_filter = test_rdd.filter(lambda x: 'a' in x or 'b' in x).collect()
    print(f"RDD filtered content: {data_filter}")

    first_line = test_rdd.first()
    print(f"first element: {first_line}")

    data_sample = test_rdd.sample(withReplacement=True, fraction=0.5).collect()
    print(data_sample)
    data_sample = test_rdd.sample(withReplacement=False, fraction=0.5).collect()
    print(data_sample)
    return


def demo2():
    """
    从本地读取文件来创建 RDD, 并解析.
    :return:
    """
    test_file_path = "file:///D:/Users/Administrator/PycharmProjects/PySpark/dataset/test.txt"
    sc = SparkContext("local", "first app")
    test_rdd = sc.textFile(test_file_path)

    file_content = test_rdd.collect()
    print(f"RDD content: {file_content}")

    def func(x):
        l = x.split(sep=' ')
        k, v = l[0], l[1]
        return k, v
    map_rdd = test_rdd.map(func)
    data_map = map_rdd.collect()
    print(f"RDD mapped content: {data_map}")

    # 将 RDD 展平.
    flat_map_rdd = map_rdd.flatMap(lambda x: (x[0], x[1]))
    data_flat_map = flat_map_rdd.collect()
    print(f"flat map content: {data_flat_map}")

    # 获取 RDD 中的不重复值. numPartitions 好像是指并行数.
    map_rdd_distinct = map_rdd.distinct(numPartitions=1)
    print(f"map rdd distinct: {map_rdd_distinct.collect()}")

    flat_map_rdd_distinct = flat_map_rdd.distinct(numPartitions=1)
    print(f"flat map rdd distinct: {flat_map_rdd_distinct.collect()}")

    # 将 RDD 按 key 进行降序排序, 然后获取其中的前 num 个元素, 返回值为列表, 不是 RDD.
    top_num = map_rdd.top(num=4, key=lambda x: x[1])
    print(f"top num: {top_num}")

    # 获取 RDD 中的前 num 个元素, 返回值为列表, 不是 RDD.
    take_num = map_rdd.take(num=3)
    print(f"take num: {take_num}")
    return


def demo3():
    the_path = os.getcwd()
    test_file_path = os.path.join(the_path, '..', 'dataset/VS14MORT/VS14MORT.txt.gz')
    rdd_file_path = 'file:///' + test_file_path
    print(rdd_file_path)

    sc = SparkContext("local", "first app")
    test_rdd = sc.textFile(rdd_file_path)
    parsed_rdd = test_rdd.map(extract_information)

    # 从 RDD 中获取前 num 个元素. 返回值为列表, 不是 RDD 对象
    take_num = parsed_rdd.take(num=1)
    print(take_num)

    # 将 RDD 按 key 进行降序排序, 然后获取其中的前 num 个元素, 返回值为列表, 不是 RDD. (因为此数据集太大, 所以此操作会报错).
    # top_num = parsed_rdd.top(num=1)
    # top_num = parsed_rdd.map(lambda x: x[16]).top(num=2)
    # print(d1.collect())
    return


def demo4():
    """
    RDD 连接.
    leftOuterJoin 像在 SQL 中一样, 根据两个数据集中都有的值来连接两个 RDD,
    并返回左侧的 RDD 的记录, 而右边的记录附加在两个 RDD 匹配的地方
    """
    sc = SparkContext("local", "first app")
    rdd1 = sc.parallelize([('a', 1), ('b', 4), ('c', 10)])
    rdd2 = sc.parallelize([('a', 4), ('a', 1), ('b', '6'), ('d', 15)])

    # 左连接.
    rdd3 = rdd1.leftOuterJoin(rdd2)
    print(rdd3.collect())

    # 内连接.
    rdd4 = rdd1.join(rdd2)
    print(rdd4.collect())

    # 返回两个 RDD 中相等的记录.
    rdd5 = rdd1.intersection(rdd2)
    print(rdd5.collect())
    return


def demo5():
    test_file_path = "file:///D:/Users/Administrator/PycharmProjects/PySpark/dataset/test.txt"
    sc = SparkContext("local", "first app")
    test_rdd = sc.textFile(test_file_path)

    file_content = test_rdd.collect()
    print(f"RDD content: {file_content}")

    def func(x):
        l = x.split(sep=' ')
        k, v = l[0], l[1]
        return k, v
    map_rdd = test_rdd.map(func)
    data_map = map_rdd.collect()
    print(f"RDD mapped content: {data_map}")

    # .repartition() 重新对数据集进行分区, 改变数据集分区的数量.
    map_rdd = map_rdd.repartition(numPartitions=4)
    map_rdd_glom = map_rdd.glom()
    print(map_rdd_glom.collect())
    return


def demo6():
    sc = SparkContext("local", "first app")
    # numSlices 指定将数据存储在几个分区中.
    rdd1 = sc.parallelize([('a', 4), ('a', 1), ('b', '6'), ('d', 15)], numSlices=4)
    # .glom() 方法返回 RDD 中的数据, 并使每一个分区的数据分别在一个列表中.
    print(rdd1.glom().collect())

    return


if __name__ == '__main__':
    demo6()
