import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = "s3a://udacity-dend/song_data/A/A/A/*.json"
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df['song_id', 'title', 'artist_id', 'year', 'duration']
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").mode('overwrite').parquet("songs.parquet")

    # extract columns to create artists table
    artists_table = df['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet("artist.parquet")


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = "s3a://udacity-dend/log_data/2018/11/*.json"

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df['page'] == 'NextSong')
    df = df.dropna()

    # extract columns for users table    
    users_table = df['userId', 'firstName', 'lastName', 'gender', 'level']
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet("users.parquet")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x/1000.0).timestamp)
    df = df.withColumn('timestamp', get_timestamp(df.ts))
   
    # extract columns to create time table
    time_table = df.selectExpr(
        "timestamp as start_time",
        "hour(timestamp) as hour",
        "dayofmonth(timestamp) as day",
        "weekofyear(timestamp) as week",
        "month(timestamp) as month",
        "year(timestamp) as year",
        "dayofweek(timestamp) as weekday"
     )
        
    # write time table to parquet files partitioned by year and month
    time_table.write.mode("overwrite").partitionBy("year", "month").parquet("time.parquet")

    # read in song data to use for songplays table
    song_df = spark.read.json(song_data) 

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""
    SELECT
    l.timestamp as start_time,
    year(l.timestamp) as year,
    month(l.timestamp) as month,
    l.userId AS user_id,
    l.level as level,
    s.song_id as song_id,
    s.artist_id as artist_id,
    l.sessionId as session_id,
    l.location as location,
    l.userAgent as user_agent
    FROM log_data l
    JOIN
    song_data s ON l.song = s.title AND l.artist = s.artist_name AND l.length = s.duration

""")

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").mode('overwrite').parquet("songplays.parquet")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-dend/output/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
