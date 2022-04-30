import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession .builder .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """ Load data from song_data dataset and extract columns
    for songs and artist tables and write the data into parquet
    files which will be loaded on s3.
    Parameters
    ----------
    spark: session
          This is the spark session that has been created
    input_data: path
           This is the path to the song_data s3 bucket.
    output_data: path
            This is the path to where the parquet files will be written.
    """
    
    
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*.json"
    
    # read song data file
    df_songs = spark.read.json(song_data)

    # extract columns to create songs table
    # get only the needed columns
    songs_table = df_songs.select("artist_id","song_id","title"
                                  ,"duration","year").dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.partitionBy("year","artist_id").parquet(output_data + "/songs/songs.parquet")

    # extract columns to create artists table
    artists_table =  df_songs.select("artist_id","artist_name","artist_latitude",
                                    "artist_longitude","artist_location")
                            .withColumnRenamed("artist_name",'name')
                            .withColumnRenamed("artist_latitude",'latitude')
                            .withColumnRenamed("artist_longitude",'longitude')
                            .withColumnRenamed("artist_location",'location')
    
    # write artists table to parquet files
    artists_table.parquet(output_data + "/artists_info/artists.parquet",'overwrite')


def process_log_data(spark, input_data, output_data):
    """
    Load data from log_data dataset and extract columns
    for users and time tables, reads both the log_data and song_data
    datasets and extracts columns for songplays table with the data.
    It writes the data into parquet files which will be loaded on s3.
    Parameters
    ----------
    spark: session
          This is the spark session that has been created
    input_data: path
           This is the path to the log_data s3 bucket.
    output_data: path
            This is the path to where the parquet files will be written.
    """
    
    
    # get filepath to log data file
    log_data = input_data + 'log_data/*.json'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    actions_table = df.filter(df.page == 'NextSong').select('ts', 'userId', 'level', 'song', 'artist','sessionId', 'location', 'userAgent').dropDuplicates()

    # extract columns for users table    
    users_table = df.select('userId', 'firstName', 'lastName',
                            'gender', 'level').dropDuplicates()
    
    # write users table to parquet files
    users_table.parquet(output_data + 'users/users.parquet','overwrite')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x : to_timestamp(x))
    actions_table = actions_table.withColumn("timestamp",get_timestamp(actions_table.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x : datetime.fromTimeStamp(int(x)/1000))
    actions_table = actions_table.withColumn("datetime",get_datetime(actions_table.ts))
    
    # extract columns to create time table
    time_table = actions_table.select('datetime')
                              .withColumn("start_time",actions_table.datetime)
                              .withcolumn("hour",hour(actions_table.datetime))
                              .withColumn("day",day(actions_table.datetime))
                              .withColumn("week",weekofyear(actions_table.datetime))
                              .withColumn("month",month(actions_table.datetime))
                              .withColumn("year",year(actions_table.datetime))
                              .withColumn("weekday",dayofweek(actions_table.datetime))
                              
                            
    # write time table to parquet files partitioned by year and month
    time_table.partitonBy("year",'month').parquet(output_data + "time/time.parquet")

    # read in song data to use for songplays table
    song_df = spark.read.json(input_data + 'song_data/*/*/*/*.json')

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table =  actions_table.join(song_df,song_df.artist_name == actions_table.artist,
                                         'inner') 
    
    songplays_table =  songplays_table.select(
        col('log_df.datetime').alias('start_time'),
        col('log_df.userId').alias('user_id'),
        col('log_df.level').alias('level'),
        col('song_df.song_id').alias('song_id'),
        col('song_df.artist_id').alias('artist_id'),
        col('log_df.sessionId').alias('session_id'),
        col('log_df.location').alias('location'), 
        col('log_df.userAgent').alias('user_agent'),
        year('log_df.datetime').alias('year'),
        month('log_df.datetime').alias('month')) 
        .withColumn('songplay_id', monotonically_increasing_id())
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table.partionBy("year",'Month').parquet(output_data + 'songplays/songplays.parquet')


def main():
    """
    Perform the following roles:
    1.) Get or create a spark session.
    1.) Read the song and log data from s3.
    2.) take the data and transform them to tables
    which will then be written to parquet files.
    3.) Load the parquet files on s3.
    """    
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://ah-loaded_data/"

    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
