import pandas as pd
import os
import configparser
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark import SparkConf
from pyspark.sql import types as T
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, DateType, IntegerType
from pyspark.sql.types import *
from pyspark.sql.functions import udf, col, month, year, month, dayofmonth, max, desc, upper
from datetime import datetime, timedelta
from etl_utils import code_mapper, set_schema, convert_datetime, quality_check
from etl_sql import immigration_sql, time_sql, us_cities_sql, temp_sql

config = configparser.ConfigParser()
config.read('config.cfg')
ACCESS_KEY = config['AWS']['AWS_ACCESS_KEY_ID']
SECRET_KEY = config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    print('Creating Spark Session...')
    conf = (
    SparkConf()
        .set('spark.hadoop.fs.s3a.access.key', ACCESS_KEY)
        .set('spark.hadoop.fs.s3a.secret.key', SECRET_KEY)
        .set("spark.jars.packages",\
             "saurfang:spark-sas7bdat:3.0.0-s_2.11,org.apache.hadoop:hadoop-aws:2.7.2")
        .set("spark.ui.enabled","true")
)
    sc = SparkContext(conf=conf)
    spark = SparkSession(sc).builder.enableHiveSupport().getOrCreate()
    print('Spark Session successfully created.')
    return spark, sc


def process_temperature_data(spark):
    fname = '../../data2/GlobalLandTemperaturesByCity.csv'
    temp_df = spark.read.csv(fname, header=True, inferSchema=True)

    temp_df_clean = temp_df.select(
        col("dt").alias("date").cast(T.DateType()), 
        upper(col("Country")).alias("country_name").cast(T.StringType()),
        upper(col("City")).alias("city_name").cast(T.StringType()),
        col("AverageTemperature").alias("average_temperature"), 
        col("AverageTemperatureUncertainty").alias("average_temperature_uncertainty"),
        col("Latitude").alias("latitude").cast(T.StringType()),
        col("Longitude").alias("longitude").cast(T.StringType())
)
    temp_df_clean = temp_df_clean.na.drop(subset=["average_temperature"])
    print('Temperature data successfully loaded.')
    return temp_df_clean


def process_us_cities_data(spark):
    fname = "us-cities-demographics.csv"
    us_cities_df = spark.read.csv(fname, inferSchema=True, header=True, sep=';')
    us_cities_df_clean = us_cities_df.select(
        upper(col("City")).alias("city_name").cast(T.StringType()),
        col("State Code").alias("state_code").cast(T.StringType()),
        upper(col("Race")).alias("race").cast(T.StringType()),
        col("Male Population").alias("male_population").cast(T.IntegerType()),
        col("Female Population").alias("female_population").cast(T.IntegerType()),
        col("Total Population").alias("total_population").cast(T.IntegerType()),
        col("Foreign-born").alias("foreign_born").cast(T.IntegerType()))
    print('Us cities data successfully loaded.')
    return us_cities_df_clean


def process_sas_mapping_files(spark):
    #open files
    with open('./I94_SAS_Labels_Descriptions.SAS') as f:
        f_content = f.read()
        f_content = f_content.replace('\t', '')

    #create dictionaries
    i94cit_res = code_mapper(f_content, "i94cntyl") # country code numeric to country name
    i94port = code_mapper(f_content, "i94prtl")     # city code to city name and state code
    i94mode = code_mapper(f_content, "i94model")    # travel mode
    i94addr = code_mapper(f_content, "i94addrl")    # state code to state name
    i94visa = {'1':'Business', '2': 'Pleasure', '3' : 'Student'}

    #create mapping helper tables
    country_map_df = pd.DataFrame.from_dict(i94cit_res,orient='index', columns=['country_name']).reset_index()
    country_map_df.columns = ['country_code_numeric','country_name'] 
    country_map_df = spark.createDataFrame(country_map_df)
    city_map_df = pd.DataFrame.from_dict(i94port,orient='index', columns=['city_name_and_state']).reset_index()
    city_map_df.columns = ['city_code','city_name_and_state_code'] 
    city_map_df['city_name'] = city_map_df['city_name_and_state_code'].str.split(',',n = 1, expand = True)[0]
    city_map_df['state_code'] = city_map_df['city_name_and_state_code'].str.split(',',n = 1, expand = True)[1]
    city_map_df=city_map_df[['city_code','city_name','state_code']]

    for cols in city_map_df.columns:
        city_map_df[cols] = city_map_df[cols].str.strip()
    airport_map_df = pd.DataFrame.from_dict(i94addr,orient='index',
                           columns=['state']).reset_index()
    airport_map_df.columns = ['state_code', 'state_name']

    city_map_df = city_map_df.merge(airport_map_df,how='left',on="state_code")
    city_map_df = city_map_df.astype(str)
    return country_map_df, city_map_df


def process_i94_data(spark, sc):
    #define schema
    schema = set_schema()
    #specify folder
    files = os.listdir('../../data/18-83510-I94-Data-2016/')

    #loop over all the files in folder, and union all
    i94_df_full = spark.createDataFrame(sc.emptyRDD(),schema)
    for fname in files:
        path="../../data/18-83510-I94-Data-2016/"+fname
        df =spark.read.format("com.github.saurfang.sas.spark").load(path)
        df = df.select(
          col("cicid").cast(T.IntegerType()), 
          col("arrdate").cast(T.DoubleType()), 
          col("i94cit").cast(T.StringType()), 
          col("i94res").cast(T.StringType()), 
          col("i94port").cast(T.StringType()), 
          col("i94mode").cast(T.IntegerType()),
          col("i94addr").cast(T.StringType()), 
          col("depdate").cast(T.DoubleType()), 
          col("i94bir").cast(T.IntegerType()), 
          col("i94visa").cast(T.IntegerType()), 
          col("gender").cast(T.StringType()), 
          col("airline").cast(T.StringType()), 
          col("visatype").cast(T.StringType()))
        df = df.filter(col("i94visa")==2).filter(col("i94mode") == 1)
        i94_df_full = i94_df_full.unionAll(df)
    print('I94 data successfully loaded from SAS files.')

    return i94_df_full


def clean_i94_dataset(spark, i94_data, country_map_df):
    udf_datetime_from_sas = udf(lambda x: convert_datetime(x), T.DateType())
    i94_data = i94_data.withColumn("arrival_date", udf_datetime_from_sas("arrdate"))\
        .withColumn("departure_date", udf_datetime_from_sas("depdate"))
    #join country name from mapping helper table
    spark.conf.set("spark.sql.crossJoin.enabled", "true")
    i94_data = i94_data.join(country_map_df, i94_data.i94res == \
        country_map_df.country_code_numeric)\
            .withColumnRenamed("country_name","residency_country")\
            .withColumnRenamed("country_code_numeric","res_country_code_numeric")
    i94_data = i94_data.join(country_map_df, i94_data.i94cit == \
        country_map_df.country_code_numeric)\
            .withColumnRenamed("country_name","origin_country")\
            .withColumnRenamed("country_code_numeric","origin_country_code_numeric")
    return i94_data


def create_final_tables(spark, i94_data, temp_data, us_cities_data, city_map_df):
    #immigration fact table
    i94_data.createOrReplaceTempView("i94")    
    immigration_fact_table = spark.sql(immigration_sql)
    #time dimension table
    time_dimension = spark.sql(time_sql)
    #us_cities dimension table
    city_map = spark.createDataFrame(city_map_df)        
    city_map.createOrReplaceTempView("city_map")
    us_cities_data.createOrReplaceTempView("cities")
    us_cities_dimension = spark.sql(us_cities_sql)
    #temperature dimension table
    temp_data.createOrReplaceTempView("temp")
    temp_dimension  = spark.sql(temp_sql)
    return immigration_fact_table, us_cities_dimension, temp_dimension, time_dimension


def write_to_s3(immig, us_cities, temp, time):
    print('Writing tables to S3...')
    immig.write.mode('overwrite').partitionBy("arrival_date")\
    .parquet("s3a://aws-emr-resources-926236161117-us-west-2/capstone/immigration_fact")
    us_cities.write.mode('overwrite').parquet("s3a://aws-emr-resources-926236161117-us-west-2/capstone/us_cities_dim")
    temp.write.mode('overwrite').partitionBy("date").parquet("s3a://aws-emr-resources-926236161117-us-west-2/capstone/temperature_dim")
    time.write.mode('overwrite').parquet("s3a://aws-emr-resources-926236161117-us-west-2/capstone/time_dim")
    print('Tables successfully copied to S3.')


def perform_quality_checks():
    quality_check("us_cities_dim")
    quality_check("time_dim")
    quality_check("immigration_fact")
    quality_check("temperature_dim")


def main():
    spark, sc = create_spark_session()
    temp_data = process_temperature_data(spark)
    us_cities_data = process_us_cities_data(spark)
    i94_data = process_i94_data(spark, sc)
    country_map_df = process_sas_mapping_files(spark)[0]    
    i94_data_clean = clean_i94_dataset(spark, i94_data,country_map_df)
    city_map = process_sas_mapping_files(spark)[1]
    immig, us_cities, temp, time = create_final_tables(spark, i94_data_clean, temp_data, us_cities_data, city_map)
    write_to_s3(immig, us_cities, temp, time)


if __name__ == "__main__":
        main()

