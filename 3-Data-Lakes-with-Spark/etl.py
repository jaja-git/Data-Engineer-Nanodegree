import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql import types as T


config = configparser.ConfigParser()
config.read('dl.cfg')

#os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
#os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .config("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.awsAccessKeyId", os.environ['AWS_ACCESS_KEY_ID']) \
        .config("spark.hadoop.fs.s3a.awsSecretAccessKey", os.environ['AWS_SECRET_ACCESS_KEY']) \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data_path = os.path.join(input_data,"*.json")
    
    # read song data file
    song_df = spark.read.json(song_data_path)

    # extract columns to create songs table
    song_table = song_df.select(
        "song_id",
        "title",
        "artist_id",
        "year",
        "duration").distinct()
    
    # write songs table to parquet files partitioned by year and artist
    song_table_path = os.path.join(output_data,"songs")
    song_table.write.mode("overwrite").partitionBy("year","artist_id").parquet(song_table_path)

    # extract columns to create artists table
    artist_table = song_df.select(
        "artist_id", 
        "artist_name", 
        "artist_location", 
        "artist_latitude", 
        "artist_longitude"
    ).distinct()

    
    # write artists table to parquet files
    artist_table_path = os.path.join(output_data,"artist")
    artist_table.write.mode("overwrite").parquet(artist_table_path)


def process_log_data(spark, song_input_data, log_input_data, output_data):
    # get filepath to log data file
    log_data_path = os.path.join(log_input_data,"*.json")


    # read log data file
    log_df = spark.read.json(log_data_path)
    
    # filter by actions for song plays
    log_df = log_df.filter(log_df.page == 'NextSong')

    # extract columns for users table    
    log_df.createOrReplaceTempView('logs')
    user_table = spark.sql('SELECT DISTINCT userId, firstName, lastName, gender, level FROM logs')

    
    # write users table to parquet files
    user_table_path = os.path.join(output_data,"users")
    user_table.write.mode("overwrite").parquet(os.path.join(user_table_path))

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp( (x/1000.0) ), T.TimestampType()) 
    log_df = log_df.withColumn("timestamp", get_timestamp(log_df.ts))
    
    # extract columns to create time table
    time_table = log_df.select("ts","timestamp")
    time_table.createOrReplaceTempView('time_view')
    time_table = spark.sql("""
    SELECT 
        ts,
        timestamp,
        day(timestamp) AS day,
        month(timestamp) AS month,
        year(timestamp) AS year FROM time_view
        """)
    
    # write time table to parquet files partitioned by year and month
    time_table_path = os.path.join(output_data,"time")
    time_table.write.mode("overwrite").parquet(time_table_path)

    # read in song data to use for songplays table
    song_data_path = os.path.join(song_input_data,"*.json")
    song_df = spark.read.json(song_data_path)

    # extract columns from joined song and log datasets to create songplays table
    log_df.createOrReplaceTempView('log_view')
    song_df.createOrReplaceTempView('song_view')
    songplay_table = spark.sql("""
    SELECT 
      row_number() OVER (ORDER BY l.ts) AS songplay_id,
      l.timestamp AS start_time,
      l.userId AS user_id,
      l.level,
      s.song_id,
      s.artist_id,
      l.sessionId AS session_id,
      l.location,
      l.userAgent
    FROM log_view l
    LEFT JOIN song_view s 
        ON l.artist = s.artist_name
        AND l.song = s.title""")
    
    # write songplays table to parquet files partitioned by year and month
    songplay_table_path = os.path.join(output_data,"songplays")
    songplay_table.write.mode("overwrite").parquet(songplay_table_path)


def main():
    spark = create_spark_session()
    
    song_input_data = "s3a://udacity-dend/song-data/A/A/A/"
    log_input_data = "s3a://udacity-dend/log-data/2018/11"
    output_data = "s3a://aws-emr-resources-926236161117-us-west-2/spark-dwh"
    
    process_song_data(spark, song_input_data, output_data)    
    process_log_data(spark, song_input_data, log_input_data, output_data)


if __name__ == "__main__":
    main()
