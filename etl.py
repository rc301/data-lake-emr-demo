import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

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
                                .withColumnRenamed('artist_longitude','longitude') \
                                .withColumnRenamed('sessionId','session_id') 
    
    # write artists table to parquet files
    artists_table.write \
        .option("header", True) \
        .mode("Overwrite") \
        .parquet(f"{output_data}artists_table/")


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = f"{input_data}log_data/"

    # read log data file
    log_schema = StructType( \
                    [StructField('artist', StringType(), True), \
                         StructField('auth', StringType(), True), \
                         StructField('firstName', StringType(), True), \
                         StructField('gender', StringType(), True), \
                         StructField('itemInSession', LongType(), True), \
                         StructField('lastName', StringType(), True), \
                         StructField('length', DoubleType(), True), \
                         StructField('level', StringType(), True), \
                         StructField('location', StringType(), True), \
                         StructField('method', StringType(), True), \
                         StructField('page', StringType(), True), \
                         StructField('registration', DoubleType(), True), \
                         StructField('sessionId', LongType(), True), \
                         StructField('song', StringType(), True), \
                         StructField('status', LongType(), True), \
                         StructField('ts', LongType(), True), \
                         StructField('userAgent', StringType(), True), \
                         StructField('userId', StringType(), True) \
                    ])
    df = spark.read.schema(log_schema)\
                .json(log_data) \
                .withColumnRenamed('userId', 'user_id') \
                .withColumnRenamed('firstName', 'first_name') \
                .withColumnRenamed('lastName','last_name') \
                .withColumnRenamed('sessionId','session_id') \
                .withColumnRenamed('userAgent','user_agent') 
    
    # filter by actions for song plays
    df = df.filter("page=='NextSong'") 

    # extract columns for users table    
    cols_users_table = ['userId', 'firstName', 'lastName', 'gender', 'level']
    users_table = df.select(cols_users_table).dropDuplicates(cols_users_table)
    
    # write users table to parquet files
    users_table.write \
        .option("header", True) \
        .mode("Overwrite") \
        .parquet(f"{output_data}users_table/")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x : datetime.fromtimestamp(x / 1000), TimestampType())
    df = df.withColumn('start_time', get_timestamp('ts'))
    
    # create datetime column from original timestamp column
    dt_type = StructType( \
            [StructField('hour', IntegerType(), True), \
                 StructField('day', IntegerType(), True), \
                 StructField('week', IntegerType(), True), \
                 StructField('month', IntegerType(), True), \
                 StructField('year', IntegerType(), True), \
                 StructField('weekday', IntegerType(), True) \
            ])
    def get_datetime_parts(ts):
        dt = datetime.fromtimestamp(ts / 1000)
        hour = dt.hour
        day = dt.day
        week = int(dt.strftime("%W"))
        month = dt.month
        year = dt.year
        weekday = dt.date().weekday()
        return hour, day, week, month, year, weekday
    get_datetime = udf(get_datetime_parts, dt_type)
    df = df.withColumn('dt', get_datetime('ts'))
    
    # extract columns to create time table
    time_table_cols = ['start_time', 'dt.hour', 'dt.day', 'dt.week', 'dt.month', 'dt.year', 'dt.weekday']
    time_table = df.select(time_table_cols).distinct()
    
    # write time table to parquet files partitioned by year and month
    time_table.write \
        .option("header", True) \
        .partitionBy("year", "month") \
        .mode("Overwrite") \
        .parquet(f"{output_data}time_table/")

    # read in song data to use for songplays table
    song_data = f"{input_data}song_data/*/*/*"
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
    song_cols = ['artist_id', 'artist_name', 'song_id', 'title']
    song_df = spark.read.schema(song_schema).json(song_data).select(song_cols).distinct()

    # extract columns from joined song and log datasets to create songplays table 
    log_cols = ['start_time', 'user_id', 'artist', 'song', 'level', 'session_id', 'location', 'user_agent', 'dt.year', 'dt.month']
    join_condition = ((df.artist==song_df.artist_name) & (df.song==song_df.title))
    songplays_table = df.select(log_cols) \
        .join(song_df, on=join_condition, how='left')
    
    # inserting auto incremental column 'songplay_id'    
    window = Window.orderBy('start_time')
    songplays_table = songplays_table.withColumn('songplay_id', row_number().over(window))

    # write songplays table to parquet files partitioned by year and month
    songplays_cols_final = ['songplay_id', 'start_time', 'user_id', 'level', 'song_id', 'artist_id', 'session_id', 'location', 'user_agent', 'year', 'month']
    songplays_table.select(songplays_cols_final).write \
            .option("header", True) \
            .partitionBy("year", "month") \
            .mode("Overwrite") \
            .parquet(f"{output_data}songplays_table/")


def main():
    spark = create_spark_session()
    # input_data = "s3a://udacity-dend/"
    input_data = "s3a://udacity-bkt-rc301-spark-project/data/"
    output_data = "s3://udacity-bkt-rc301-spark-project/output_data/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
