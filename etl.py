import configparser
import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek
from pyspark.sql.types import StructType, StructField as sf, IntegerType as i, StringType as st, DoubleType as dec, LongType as lt, TimestampType as tt, BooleanType as bt
import re

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    '''Initiates a spark session'''
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    '''This module loads the song data from the logs stored in S3 bucket, transforms and then writes back the parquet files to S3'''
    
    song_schema = StructType ([
        sf("num_songs",i()),
        sf("artist_id", st()),
        sf("artist_latitude", dec()),
        sf("artist_longitude", dec()),
        sf("artist_location", st()),
        sf("artist_name", st()),
        sf("song_id", st()),
        sf("title", st()),
        sf("duration", dec()),
        sf("year", i()),
        sf("_corrupt_record", st())
    ])
    
    # get filepath to song data file
    song_data = "song_data/*/*/*/*.json"
    
    # read song data file
    songs_df = spark.read.json(input_data + song_data, schema=song_schema) 
    
    # extract columns to create songs table
    songs = songs_df.select(["song_id","title","artist_id","year","duration"])
    
    # write songs table to parquet files partitioned by year and artist
    print("##### WRITING SONGS TABLE #####")
    songs.write.mode("overwrite").partitionBy('year', 'artist_id').parquet(output_data + "songs/songs.parquet")

    # extract columns to create artists table
    artists = songs_df.select(["artist_id","artist_name","artist_location","artist_latitude","artist_longitude"]) 
    
    # write artists table to parquet files
    print("##### WRITING ARTISTS TABLE #####")
    artists.write.mode("overwrite").parquet(output_data + "artists/artists.parquet")
    
    return songs_df

def process_log_data(spark, input_data, output_data, songs_df):
    '''This module loads the song data from the logs stored in S3 bucket, transforms and then writes back the parquet files to S3'''
    
    logSchema = StructType(        
     [            
      sf('artist', st()),            
      sf('auth', st()),            
      sf('firstName', st()),            
      sf('gender', st()),            
      sf('itemInSession', st()),            
      sf('lastName', st()),            
      sf('length', dec()),            
      sf('level', st()),            
      sf('location', st()),            
      sf('method', st()),            
      sf('page', st()),            
      sf('registration', dec()),            
      sf('sessionId', i()),            
      sf('song', st()),            
      sf('status', i()),            
      sf('ts', lt()),            
      sf('userAgent', st()),            
      sf('userId', st()),
      sf('_corrupt_record', st())
     ]    
    )
    # get filepath to log data file
    log_data = "log-data/*/*/*.json"

    # read log data file
    log_df = spark.read.json(input_data + log_data, schema=logSchema)
    
    users = log_df.select(["userId","firstName","lastName","gender","level"])
    print("##### WRITING USERS TABLE #####")
    users.write.mode("overwrite").parquet(output_data + "users/users.parquet")
    
    log_df.createOrReplaceTempView("time_table")
    
    spark.udf.register("get_start_time",lambda x:datetime.datetime.fromtimestamp(x//1000), tt())
    
    log_df = spark.sql('''
        SELECT artist,
        auth,            
        firstName,            
        gender,            
        itemInSession,            
        lastName,            
        length,            
        level,            
        location,            
        method,            
        page,            
        registration,            
        sessionId,            
        song,            
        status,            
        ts,            
        userAgent,            
        userId,
        get_start_time(ts) AS start_time,
        hour(get_start_time(ts)) AS hour,
        dayofmonth(get_start_time(ts)) AS day, 
        month(get_start_time(ts)) AS month, 
        year(get_start_time(ts)) AS year, 
        weekofyear(get_start_time(ts)) AS week,
        CASE WHEN dayofweek(get_start_time(ts)) NOT IN (0, 7) THEN true ELSE false END AS weekday
        FROM time_table''')
    
    time = log_df.select(["start_time","hour","day","week","month","year","weekday"])
    print("##### WRITING TIME TABLE #####")
    time.write.mode("overwrite").partitionBy('year', 'month').parquet(output_data + "time/time.parquet")
    
    songs_df = songs_df.selectExpr(["song_id","title","artist_id","artist_name"])
    log_df = log_df.withColumn("songplay_id", monotonically_increasing_id())
    log_df.createOrReplaceTempView("log_df")
    songs_df.createOrReplaceTempView("songs_df")
    songplays = spark.sql('''
        SELECT log_df.songplay_id, 
        log_df.page, 
        log_df.start_time, 
        log_df.userId, 
        log_df.level, 
        songs_df.song_id, 
        songs_df.artist_id, 
        log_df.sessionId, 
        log_df.location, 
        log_df.userAgent, 
        log_df.year,
        log_df.month 
        FROM log_df INNER JOIN songs_df ON (log_df.song = songs_df.title) AND (log_df.artist = songs_df.artist_name) 
        WHERE log_df.page = 'NextSong' ''')
    
    print("##### WRITING SONGPLAYS TABLE #####")
    songplays.write.mode("overwrite").partitionBy('year', 'month').parquet(output_data + "songplays/songplays.parquet")

def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://datalake-udacity/analytics/"
    
    songs_df = process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data, songs_df)


if __name__ == "__main__":
    main()
