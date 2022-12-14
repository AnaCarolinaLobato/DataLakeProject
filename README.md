# Project: Data Lake

## Introduction:
A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

## Description of the project:
The project on Spark and data lakes to build an ETL pipeline for a data lake hosted on S3. To complete the project, you will need to load data from S3, process the data into analytics tables using Spark, and load them back into S3. You'll deploy this Spark process on a cluster using AWS.
 
## The Dataset:
This project used two Dataset:
- Song Dataset - is a subset of real data from the Million Song Dataset. Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID
- Log Dataset - consists of log files in JSON format generated by this event simulator based on the songs in the dataset above. These simulate activity logs from a music streaming app based on specified configurations.

## Starting the project:
Using the dataset song and log to create a star schema optimized for queries on song play analysis. This includes the following tables:

- Fact Table

songplays - records in log data associated with song plays i.e. records with page NextSong

```sh
songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
```

- Dimension Tables

users - users in the app

```sh
user_id, first_name, last_name, gender, level
``` 

songs - songs in music database

```sh
song_id, title, artist_id, year, duration
```

artists - artists in music database

```sh
artist_id, name, location, latitude, longitude
```

time - timestamps of records in songplays broken down into specific units

```sh
start_time, hour, day, week, month, year, weekday
```

## Steps:
- Complete the logic in etl.py to connect to the database and create tables, parquet file, parquet files partitions, convert to timestamp, and join song and log datasets, to processes in S3.
- Add your AWS credetials keys to dl.cfg.
- launch EMR Cluster and S3 notebook in AWS.
