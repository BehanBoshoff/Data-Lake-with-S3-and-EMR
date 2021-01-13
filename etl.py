import configparser
from datetime import datetime
import os
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import Window


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
        CREATES SPARK SESSION
    """
    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
        LOADS SONG DATA FROM S3, CREATES SONG TABLE AND ARTISTS TABLE, AND WRITES BOTH TABLES INTO PARQUET
    """
    
    # get filepath to song data file
    song_data = input_data + "song_data/A/A/B/*.json"
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    start = time.time()
    songs_table = df.select("song_id", "title", "artist_id", "year", "duration").distinct()
    end = time.time()
    print('extract columns to create songs table runtime (s):', end-start)
    
    # write songs table to parquet files partitioned by year and artist
    start = time.time()
    songs_table.write.mode('append').partitionBy("year", "artist_id").parquet(output_data + "songs/")
    end = time.time()
    print('write songs table to parquet files runtime (s):', end-start)

    # extract columns to create artists table
    start = time.time()
    artists_table = df.select("artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude").distinct()
    end = time.time()
    print('extract columns to create artists table runtime (s):', end-start)
    
    # write artists table to parquet files
    start = time.time()
    artists_table.write.mode('append').partitionBy("artist_name", "artist_id").parquet(output_data + "artists/")
    end = time.time()
    print('write artists table to parquet files runtime (s):', end-start)


def process_log_data(spark, input_data, output_data):
    """
        LOADS LOG DATA FROM S3 THEN CREATES USERS TABLE, TIME TABLE, AND SONGPLAYS TABLE, AND WRITES TABLES INTO PARQUET
    """
    
    # get filepath to log data file
    log_data = input_data + "log_data/2018/11/*.json"

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    start = time.time()
    users_table = df.select("userId", "firstName", "lastName", "gender", "level").distinct()
    end = time.time()
    print('extract columns to create users table runtime (s):', end-start)
    
    # write users table to parquet files
    start = time.time()
    users_table.write.mode('append').partitionBy("userId").parquet(output_data + "users/")
    end = time.time()
    print('write users table to parquet files runtime (s):', end-start)
    
    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x:  datetime.fromtimestamp(x/1000).strftime('%Y-%m-%d %H:%M:%S'))
    df = df.withColumn("start_time", get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x/1000).strftime('%Y-%m-%d'))
    df = df.withColumn("datetime", get_datetime(df.ts))

    # extract columns to create time table
    start = time.time()
    time_table = df.withColumn("hour", hour(col("start_time")))\
        .withColumn("day", dayofmonth(col("start_time")))\
        .withColumn("week", weekofyear(col("start_time")))\
        .withColumn("month", month(col("start_time")))\
        .withColumn("year", year(col("start_time")))\
        .withColumn("weekday", dayofweek(col("start_time")))\
        .select("start_time", "day", "week", "month", "year", "weekday") \
        .drop_duplicates()
    end = time.time()
    print('extract columns to create time table runtime (s):', end-start)
    
    # write time table to parquet files partitioned by year and month
    start = time.time()
    time_table.write.mode('append').partitionBy("year", "month").parquet(output_data + "time/")
    end = time.time()
    print('write time table to parquet files runtime (s):', end-start)

    # read in song data to use for songplays table
    song_df = spark.read\
                .format("parquet")\
                .option("basePath", os.path.join(output_data, "songs/"))\
                .load(os.path.join(output_data, "songs/*/*/"))
    
    # extract columns from joined song and log datasets to create songplays table 
    window = Window.orderBy(col('start_time'))
    start = time.time()
    songplays_table = df.withColumn('songplay_id', row_number().over(window))\
        .join(song_df, (df.song == song_df.title) & (df.artist == song_df.artist_id) & (df.length == song_df.duration), 'left_outer')\
        .select(
            col('start_time'),
            col("userId").alias('user_id'),
            df.level,
            song_df.song_id,
            song_df.artist_id,
            col("sessionId").alias("session_id"),
            df.location,
            col("useragent").alias("user_agent"),
            year('datetime').alias('year'),
            month('datetime').alias('month'))
    end = time.time()
    print('extract columns to create songplays table runtime (s):', end-start)

    # write songplays table to parquet files partitioned by year and month
    start = time.time()
    songplays_table.write.mode('append').partitionBy("year", "month").parquet(output_data + "songplays/")
    end = time.time()
    print('write songplays table to parquet files runtime (s):', end-start)


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://behan-sparkify/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
