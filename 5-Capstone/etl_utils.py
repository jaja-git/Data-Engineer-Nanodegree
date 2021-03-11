# import pandas as pd
import os
# import configparser
# from pyspark.sql import SparkSession
# from pyspark.context import SparkContext
# from pyspark import SparkConf
from pyspark.sql import types as T
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, DateType, IntegerType
# from pyspark.sql.functions import udf, col, month, year, month, dayofmonth, max, desc, upper
from datetime import datetime, timedelta


def code_mapper(file, idx):
    """Creates a dict from SAS mapping files"""
    with open('./I94_SAS_Labels_Descriptions.SAS') as f:
        f_content = f.read()
        f_content = f_content.replace('\t', '')
    f_content2 = f_content[f_content.index(idx):]
    f_content2 = f_content2[:f_content2.index(';')].split('\n')
    f_content2 = [i.replace("'", "") for i in f_content2]
    dic = [i.split('=') for i in f_content2[1:]]
    dic = dict([i[0].strip(), i[1].strip()] for i in dic if len(i) == 2)
    return dic



def set_schema():
    """Set schema for i94 file"""
    schema = StructType([
        StructField("cicid",DoubleType(),True),
        StructField("arrdate",DoubleType(),True),
        StructField("i94cit",DoubleType(),True),
        StructField("i94res",DoubleType(),True),
        StructField("i94port",StringType(),True),
        StructField("i94mode",DoubleType(),True),
        StructField("i94addr",StringType(),True),
        StructField("depdate",DoubleType(),True),    
        StructField("i94bir",DoubleType(),True),
        StructField("i94visa",DoubleType(),True),
        StructField("gender",StringType(),True),
        StructField("airline",StringType(),True),
        StructField("visatype",StringType(),True)])
    return schema


def convert_datetime(x):
    """ Converts SAS date column to datetime"""
    try:
        start = datetime(1960, 1, 1)
        return start + timedelta(days=int(x))
    except:
        return None


def quality_check(spark, folder):
    """Makes sure that tables have been copied"""
    df = spark.read.parquet("s3a://aws-emr-resources-926236161117-us-west-2/capstone/"+folder)
    result = df.count()
    if result == 0:
        raise ValueError("Data quality check failed for {} with zero records".format(df))
    else:
        print("Data quality check passed for {} with {} records".format(df, result))