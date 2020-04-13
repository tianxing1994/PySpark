# -*- coding:utf-8 -*-
"""
http://spark.apache.org/docs/latest/api/python/pyspark.mllib.html
https://github.com/drabastomek/learningPySpark

数据下载:
http://www.cdc.gov/nchs/data_access/vitalstatsonline.htm
http://www.tomdrabas.com/data/LearningPySpark/births_train.csv.gz
"""
import os
os.environ['PYSPARK_PYTHON'] = r'C:\ProgramData\Anaconda3\envs\PySpark\python.exe'

import numpy as np
import pyspark.mllib.stat as st
from pyspark.sql import SparkSession
import pyspark.sql.types as types
import pyspark.sql.functions as func


labels = [
    ('INFANT_ALIVE_AT_REPORT', types.StringType(), '婴儿是否存活, Y/N'),
    ('BIRTH_YEAR', types.IntegerType(), '出生年'),
    ('BIRTH_MONTH', types.IntegerType(), '出生月'),
    ('BIRTH_PLACE', types.StringType(), '出生地'),
    ('MOTHER_AGE_YEARS', types.IntegerType(), '母亲年龄'),
    ('MOTHER_RACE_6CODE', types.StringType(), '母亲种族'),
    ('MOTHER_EDUCATION', types.StringType(), '母亲教育程度'),
    ('FATHER_COMBINED_AGE', types.IntegerType(), '父亲综合年龄'),
    ('FATHER_EDUCATION', types.StringType(), '父亲教育程度'),
    ('MONTH_PRECARE_RECODE', types.StringType(), '月保费编码'),
    ('CIG_BEFORE', types.IntegerType()),
    ('CIG_1_TRI', types.IntegerType()),
    ('CIG_2_TRI', types.IntegerType()),
    ('CIG_3_TRI', types.IntegerType()),
    ('MOTHER_HEIGHT_IN', types.IntegerType(), '母亲身高'),
    ('MOTHER_BMI_RECODE', types.IntegerType()),
    ('MOTHER_PRE_WEIGHT', types.IntegerType(), '母亲体重'),
    ('MOTHER_DELIVERY_WEIGHT', types.IntegerType(), '产妇体重'),
    ('MOTHER_WEIGHT_GAIN', types.IntegerType(), '母亲体重增加'),
    ('DIABETES_PRE', types.StringType(), '是否糖尿病'),
    ('DIABETES_GEST', types.StringType(), '糖尿病'),
    ('HYP_TENS_PRE', types.StringType()),
    ('HYP_TENS_GEST', types.StringType()),
    ('PREV_BIRTH_PRETERM', types.StringType()),
    ('NO_RISK', types.StringType()),
    ('NO_INFECTIONS_REPORTED', types.StringType()),
    ('LABOR_IND', types.StringType()),
    ('LABOR_AUGM', types.StringType()),
    ('STEROIDS', types.StringType()),
    ('ANTIBIOTICS', types.StringType()),
    ('ANESTHESIA', types.StringType()),
    ('DELIV_METHOD_RECODE_COMB', types.StringType()),
    ('ATTENDANT_BIRTH', types.StringType()),
    ('APGAR_5', types.IntegerType()),
    ('APGAR_5_RECODE', types.StringType()),
    ('APGAR_10', types.IntegerType()),
    ('APGAR_10_RECODE', types.StringType()),
    ('INFANT_SEX', types.StringType()),
    ('OBSTETRIC_GESTATION_WEEKS', types.IntegerType()),
    ('INFANT_WEIGHT_GRAMS', types.IntegerType()),
    ('INFANT_ASSIST_VENTI', types.StringType()),
    ('INFANT_ASSIST_VENTI_6HRS', types.StringType()),
    ('INFANT_NICU_ADMISSION', types.StringType()),
    ('INFANT_SURFACANT', types.StringType()),
    ('INFANT_ANTIBIOTICS', types.StringType()),
    ('INFANT_SEIZURES', types.StringType()),
    ('INFANT_NO_ABNORMALITIES', types.StringType()),
    ('INFANT_ANCEPHALY', types.StringType()),
    ('INFANT_MENINGOMYELOCELE', types.StringType()),
    ('INFANT_LIMB_REDUCTION', types.StringType()),
    ('INFANT_DOWN_SYNDROME', types.StringType()),
    ('INFANT_SUSPECTED_CHROMOSOMAL_DISORDER', types.StringType()),
    ('INFANT_NO_CONGENITAL_ANOMALIES_CHECKED', types.StringType()),
    ('INFANT_BREASTFED', types.StringType())
]

schema = types.StructType([
        types.StructField(e[0], e[1], False) for e in labels
    ])

spark = SparkSession.builder.appName('first ss app').getOrCreate()

file_path = 'file:///D:/Users/Administrator/PycharmProjects/PySpark/dataset/others/births_train/births_train.csv.gz'

births = spark.read.csv(file_path, header=True, schema=schema)

print(f"origin csv data: ")
births.show(3)


selected_features = [
    'INFANT_ALIVE_AT_REPORT',
    'BIRTH_PLACE',
    'MOTHER_AGE_YEARS',
    'FATHER_COMBINED_AGE',
    'CIG_BEFORE',
    'CIG_1_TRI',
    'CIG_2_TRI',
    'CIG_3_TRI',
    'MOTHER_HEIGHT_IN',
    'MOTHER_PRE_WEIGHT',
    'MOTHER_DELIVERY_WEIGHT',
    'MOTHER_WEIGHT_GAIN',
    'DIABETES_PRE',
    'DIABETES_GEST',
    'HYP_TENS_PRE',
    'HYP_TENS_GEST',
    'PREV_BIRTH_PRETERM'
]

births_trimmed = births.select(selected_features)

print(f"selected features data: ")
births_trimmed.show(3)


recode_dictionary = {
    'YNU': {
        'Y': 1,
        'N': 0,
        'U': 0
    }
}


def recode(col, key):
    """
    大量的特征, 值为 Yes/No/Unknown.
    将 Yes 编码为 1, 其它值为 0.
    """
    return recode_dictionary[key][col]


def correct_cig(feature):
    """
    母亲吸烟数量:
    0 表示没有吸烟,
    1 - 97 表示实际吸烟数量,
    98 表示实际吸烟数量大于等于 98.
    99 表示实际吸烟数量未知.

    将 99 编码为 0, 其它仍是其原来的值.
    """
    return func.when(func.col(feature) != 99, func.col(feature)).otherwise(0)


rec_integer = func.udf(recode, types.IntegerType())


births_transformed = births_trimmed.\
    withColumn('CIG_BEFORE', correct_cig('CIG_BEFORE')).\
    withColumn('CIG_1_TRI', correct_cig('CIG_1_TRI')).\
    withColumn('CIG_2_TRI', correct_cig('CIG_2_TRI')).\
    withColumn('CIG_3_TRI', correct_cig('CIG_3_TRI'))


cols = [(col.name, col.dataType) for col in births_trimmed.schema]
ynu_cols = []

for i, s in enumerate(cols):
    if isinstance(s[1], types.StringType):
        dis = births.select(s[0]).distinct().rdd.map(lambda row: row[0]).collect()
        if 'Y' in dis:
            ynu_cols.append(s[0])

print(f"YNU 列: ")
print(ynu_cols)

births.select(['INFANT_NICU_ADMISSION', rec_integer('INFANT_NICU_ADMISSION', func.lit('YNU')).alias('INFANT_NICU_ADMISSION_RECODE')]).show(5)


exprs_ynu = [
    rec_integer(x, func.lit('YNU')).alias(x)
    if x in ynu_cols
    else x
    for x in births_transformed.columns
]

births_transformed = births_transformed.select(exprs_ynu)


births_transformed.select(ynu_cols[-5:]).show(5)

numeric_cols = [
    'MOTHER_AGE_YEARS',
    'FATHER_COMBINED_AGE',
    'CIG_BEFORE',
    'CIG_1_TRI',
    'CIG_2_TRI',
    'CIG_3_TRI',
    'MOTHER_HEIGHT_IN',
    'MOTHER_PRE_WEIGHT',
    'MOTHER_DELIVERY_WEIGHT',
    'MOTHER_WEIGHT_GAIN'
]

numeric_rdd = births_transformed.select(numeric_cols).rdd.map(lambda row: [e for e in row])
mllib_stats = st.Statistics.colStats(numeric_rdd)
for col, m, v in zip(numeric_cols, mllib_stats.mean(), mllib_stats.variance()):
    print('{0}: \t{1:.2f} \t {2:.2f}'.format(col, m, np.sqrt(v)))
