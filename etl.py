import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import *

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = f"{input_data}song_data/*/*/*"
    
    # read song data file
    song_schema = StructType([StructField('artist_id', StringType(), True), \
                            StructField('artist_latitude', DoubleType(), True), \
                            StructField('artist_location', StringType(), True), \
                            StructField('artist_longitude', DoubleType(), True), \
                            StructField('artist_name', StringType(), True), \
                            StructField('duration', DoubleType(), True), \
                            StructField('num_songs', LongType(), True), \
                            StructField('song_id', StringType(), True), \
                            StructField('title', StringType(), True), \
                            StructField('year', IntegerType(), True) \
                         ])
    df = spark.read.schema(song_schema).json(song_data)

    # extract columns to create songs table
    cols_songs_table = ['song_id', 'title', 'artist_id', 'year', 'duration']
    songs_table = df.select(cols_songs_table).dropDuplicates(cols_songs_table)
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write \
        .option("header", True) \
        .partitionBy("year", "artist_id") \
        .mode("Overwrite") \
        .parquet(f"{output_data}songs_table/")

    # extract columns to create artists table
    cols_artists_table = ['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']
    artists_table = df.select(cols_artists_table).dropDuplicates(cols_artists_table)
    artists_table = artists_table.withColumnRenamed('artist_name', 'name') \
                                .withColumnRenamed('artist_location', 'location') \
                                .withColumnRenamed('artist_latitude','latitude') \
                                .withColumnRenamed('artist_longitude','longitude')
    
    # write artists table to parquet files
    artists_table.write \
        .option("header", True) \
        .mode("Overwrite") \
        .parquet(f"{output_data}artists_table/")


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data =

    # read log data file
    df = 
    
    # filter by actions for song plays
    df = 

    # extract columns for users table    
    artists_table = 
    
    # write users table to parquet files
    artists_table

    # create timestamp column from original timestamp column
    get_timestamp = udf()
    df = 
    
    # create datetime column from original timestamp column
    get_datetime = udf()
    df = 
    
    # extract columns to create time table
    time_table = 
    
    # write time table to parquet files partitioned by year and month
    time_table

    # read in song data to use for songplays table
    song_df = 

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = 

    # write songplays table to parquet files partitioned by year and month
    songplays_table


def main():
    spark = create_spark_session()
    # input_data = "s3a://udacity-dend/"
    input_data = "s3a://udacity-bkt-rc301-spark-project/data/"
    output_data = "s3://udacity-bkt-rc301-spark-project/output_data/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
