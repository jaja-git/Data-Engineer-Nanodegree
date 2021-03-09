import configparser
import os
from datetime import datetime, timedelta
import pandas as pd
from pyspark import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import types as T
from pyspark.sql.functions import (col, dayofmonth, desc, max, month, udf,
                                   upper, year)
from pyspark.sql.types import *

config = configparser.ConfigParser()
config.read('config.cfg')
ACCESS_KEY = config['AWS']['AWS_ACCESS_KEY_ID']
SECRET_KEY = config['AWS']['AWS_SECRET_ACCESS_KEY']

conf = (
    SparkConf()
        .set('spark.hadoop.fs.s3a.access.key', ACCESS_KEY)
        .set('spark.hadoop.fs.s3a.secret.key', SECRET_KEY)
        .set("spark.jars.packages","saurfang:spark-sas7bdat:3.0.0-s_2.11,org.apache.hadoop:hadoop-aws:2.7.2")
        .set("spark.ui.enabled","true")
)


print('Creating Spark Session...')

sc = SparkContext(conf=conf)
spark = SparkSession(sc).builder.enableHiveSupport().getOrCreate()

print('Spark Session successfully created!')


#### GET/CLEAN TEMPERATURE DATA ####
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


print('Temperature data successfully loaded!')


#### GET/CLEAN US CITIES DATA ####
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


print('Us cities data successfully loaded!')


#### GET/CLEAN SAS MAPPING FILES ####

#open files
with open('./I94_SAS_Labels_Descriptions.SAS') as f:
    f_content = f.read()
    f_content = f_content.replace('\t', '')

    
def code_mapper(file, idx):
    f_content2 = f_content[f_content.index(idx):]
    f_content2 = f_content2[:f_content2.index(';')].split('\n')
    f_content2 = [i.replace("'", "") for i in f_content2]
    dic = [i.split('=') for i in f_content2[1:]]
    dic = dict([i[0].strip(), i[1].strip()] for i in dic if len(i) == 2)
    return dic

#create dictionaries
i94cit_res = code_mapper(f_content, "i94cntyl") # country code numeric to country name
i94port = code_mapper(f_content, "i94prtl")     # city code to city name and state code
i94mode = code_mapper(f_content, "i94model")    # travel mode
i94addr = code_mapper(f_content, "i94addrl")    # state code to state name
i94visa = {'1':'Business', '2': 'Pleasure', '3' : 'Student'}


#create mapping helper tables
country_map_df = pd.DataFrame.from_dict(i94cit_res,orient='index',
                       columns=['country_name']).reset_index()
country_map_df.columns = ['country_code_numeric','country_name'] 
country_map_df = spark.createDataFrame(country_map_df)

city_map_df = pd.DataFrame.from_dict(i94port,orient='index',
                       columns=['city_name_and_state']).reset_index()
city_map_df.columns = ['city_code','city_name_and_state_code'] 
city_map_df['city_name'] = city_map_df['city_name_and_state_code'].str.split(',',n = 1, expand = True)[0]
city_map_df['state_code'] = city_map_df['city_name_and_state_code'].str.split(',',n = 1, expand = True)[1]
city_map_df=city_map_df[['city_code','city_name','state_code']]

for cols in city_map_df.columns:
    city_map_df[cols] = city_map_df[cols].str.strip()

airport_map_df = pd.DataFrame.from_dict(i94addr,orient='index',
                       columns=['state']).reset_index()
airport_map_df.columns = ['state_code', 'state_name']
    
city_dim = city_map_df.merge(airport_map_df,how='left',on="state_code")
city_dim = city_dim.astype(str)

print('Helper tables for i94 successfully loaded!')


#### GET/CLEAN FULL I94 DATA ####


print('Loading i94 full data...')

#define schema
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

#specify folder
files = os.listdir('../../data/18-83510-I94-Data-2016/')

#loop over all the files in folder, and union all
i94_df_full = spark.createDataFrame(sc.emptyRDD(),schema)
for fname in files:
    path="../../data/18-83510-I94-Data-2016/"+fname
    df =spark.read.format("com.github.saurfang.sas.spark").load(path)
    df = df.select(
      col("cicid").alias("id").cast(T.IntegerType()), 
      col("arrdate").alias("arrival_date").cast(T.DoubleType()), 
      col("i94cit").cast(T.StringType()), 
      col("i94res").cast(T.StringType()), 
      col("i94port").alias("arrival_city_code").cast(T.StringType()), 
      col("i94mode").alias("travel_mode").cast(T.IntegerType()),
      col("i94addr").alias("arrival_state_code").cast(T.StringType()), 
      col("depdate").alias("departure_date").cast(T.DoubleType()), 
      col("i94bir").alias("age").cast(T.IntegerType()), 
      col("i94visa").alias("reason").cast(T.IntegerType()), 
      col("gender").alias("gender").cast(T.StringType()), 
      col("airline").alias("airline").cast(T.StringType()), 
      col("visatype").alias("visa_type").cast(T.StringType()))
    i94_df_full = i94_df_full.unionAll(df)

    
print('i94 data successfully loaded from SAS files!')

print('Writing to local disk as parquet...')

#write to parquet
i94_df_full.write.mode('overwrite').parquet("sas_data")

print('Success!')

#read from parquet
i94_df_full=spark.read.parquet("sas_data")


print('Cleaning dataset...')


def convert_datetime(x):
    try:
        start = datetime(1960, 1, 1)
        return start + timedelta(days=int(x))
    except:
        return None
    
udf_datetime_from_sas = udf(lambda x: convert_datetime(x), T.DateType())


#convert date columns with udf
i94_df_full = i94_df_full\
.withColumn("arrival_date", udf_datetime_from_sas("arrdate"))\
.withColumn("departure_date", udf_datetime_from_sas("depdate"))

#join country name from mapping helper table
spark.conf.set("spark.sql.crossJoin.enabled", "true")
i94_df_full = i94_df_full.join(country_map_df, i94_df_full.i94res == country_map_df.country_code_numeric).withColumnRenamed("country_name","residency_country").withColumnRenamed("country_code_numeric","res_country_code_numeric")
i94_df_full = i94_df_full.join(country_map_df, i94_df_full.i94cit == country_map_df.country_code_numeric).withColumnRenamed("country_name","origin_country").withColumnRenamed("country_code_numeric","origin_country_code_numeric")

print('Success!')

#### CREATE TABLES ####


print('Creating final tables...')

# create immigration fact table
i94_df_full.createOrReplaceTempView("i94")    
immigration_fact_table = spark.sql("""
    SELECT 
      id, 
      arrival_date,
      i94cit as origin_country,
      i94res as residency_country,
      arrival_city_code,
      travel_mode,
      age,
      reason,
      gender,
      airline,
      visa_type
    FROM i94 
""")


#create time dimension table
time_dimension = spark.sql("""
    SELECT
      arrival_date AS date,
      year(arrival_date) AS year,
      month(arrival_date) AS month,
      dayofmonth(arrival_date) AS day
    FROM i94
    GROUP BY 1,2,3,4
    ORDER BY date DESC
""")


#create us_cities dimension table
city_dim = spark.createDataFrame(city_dim)
city_dim.createOrReplaceTempView("city_dim")
us_cities_df_clean.createOrReplaceTempView("cities")
us_cities_df_clean.limit(5).toPandas()

us_cities_dimension = spark.sql("""

WITH city_pop AS (
    SELECT 
      state_code,
      city_name,
      SUM(male_population) AS male_population ,
      SUM(female_population) AS female_population,
      SUM(total_population) AS total_population,
      SUM(foreign_born) AS foreign_born
    FROM cities
    GROUP BY 1,2
)
SELECT 
  city_code,
  city_name,
  state_code,
  state_name,
  male_population,
  female_population,
  total_population,
  foreign_born
FROM city_pop 
LEFT JOIN city_dim
USING(city_name,state_code)
WHERE city_code IS NOT NULL
""")


#create temperature dimension table
temp_df_clean.createOrReplaceTempView("temp")
temp_dimension  = spark.sql("""
  SELECT
    country_name AS country,
    date,
    AVG(average_temperature) AS average_temperature
    FROM temp
    GROUP BY 1,2
    ORDER BY date DESC, country
""")


print('Tables successfully created!')

#### WRITE TO S3 ####

print('Writing tables to S3...')

immigration_fact_table.write.mode('overwrite').partitionBy("arrival_date").parquet("s3a://aws-emr-resources-926236161117-us-west-2/capstone/immigration_fact")

us_cities_dimension.write.mode('overwrite').parquet("s3a://aws-emr-resources-926236161117-us-west-2/capstone/us_cities_dim")

temp_dimension.write.mode('overwrite').partitionBy("date").parquet("s3a://aws-emr-resources-926236161117-us-west-2/capstone/temperature_dim")

time_dimension.write.mode('overwrite').parquet("s3a://aws-emr-resources-926236161117-us-west-2/capstone/time_dim")

print('Tables successfully copied to S3!')


#### READING BACK FROM S3 ####

# Perform quality checks here
def quality_check(folder):
    """ Makes sure that records have been copied to S3"""
    df = spark.read.parquet("s3a://aws-emr-resources-926236161117-us-west-2/capstone/"+folder)
    result = df.count()
    if result == 0:
        raise ValueError("Data quality check failed for {} with zero records".format(df))
    else:
        print("Data quality check passed for {} with {} records".format(df, result))
        
quality_check("us_cities_dim")
quality_check("time_dim")
quality_check("immigration_fact")
quality_check("temperature_dim")
