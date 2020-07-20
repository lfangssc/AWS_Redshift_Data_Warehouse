# AWS_Redshift_Data_Warehouse
# PROJECT: Build a Data Warehouse on AWS REDSHIFT

## Introduction
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app. 

The goal of the project is to use the provided data in S3 and ETL pipeline to build a Redshift data warehouse that can be efficiently queried for business purposes.

## The workflow of the project: 
1. Setup a Redshift dataware house in AWS using the infrastruce-as-code (IaC) method. 
2. Create the tables for the stagint log_data, staging song_data. Also Create star-schema tables in Redshift. The fact table is "songplays". The dimension tables are "users", "songs", "artists" and "time".
3. Copy the corresponding data in S3 and write in the staging tables.
4. Build ETL to extract data from staging tables and write in the fact and dimension tables.

## The scripts in the project
#### 1. Redshift_cluster_IaC
This is the main line of the project. Other scripts 'create_tables.py', 'sql_queries.py' and 'etl.py' was imported to it to reach the project target. 
It first creates an IAM role and a cluster in AWS Redshift. Then it runs the create_tables.py to create tables for stagint log_data, staging song_data as well as the star-schema including the fact table "songplays" and the dimension tables "users", "songs", "artists" and "time". After that, it runs the etl.py to fill up the staging tables and extract data from them and write in the fact and dimension tables. At last, it deletes the IAM role and the cluster.
#### 2. Create_tables.py
It is used to create the staging tables and the star-schema including the fact table "songplays" and the dimension tables "users", "songs", "artists" and "time".
It first read the parameters from the dwh.cfg and connect to the Redshift cluster, then read the queries from sql_queries.py to commit them and create tables.

#### 3.etl.py
Similar with the creat_tables.py, it first read the parameters from the dwh.cfg and connect to the Redshift cluster. Then, it loads the staging tables and the star schema. The SQL command are imported from the sql_queries.py. 

#### 4. sql_queries.py
It contains all the SQL queries for creating tables, copy staging tables and insert into the star-schema including the fact table "songplays" and the dimension tables "users", "songs", "artists" and "time".

#### 5. dwh.cfg
It's the configuration file of the AWS Redshift cluster. It has all the parameters for setting up the IAM role and the cluster, as well as the end_point and rolearn to connect to the cluster.



Sample of song_data in S3, SONG_DATA='s3://udacity-dend/song_data'
{"artist_id":"ARJNIUY12298900C91","artist_latitude":null,"artist_location":"","artist_longitude":null,"artist_name":"Adelitas Way","duration":213.9424,"num_songs":1,"song_id":"SOBLFFE12AF72AA5BA","title":"Scream","year":2009}


Sample of log_data in S3, LOG_DATA='s3://udacity-dend/log_data'
{"artist":null,
"auth":"Logged In",
"firstName":"Walter",
"gender":"M",
"itemInSession":0,
"lastName":"Frye",
"length":null,
"level":"free",
"location":"San Francisco-Oakland-Hayward, CA",
"method":"GET",
"page":"Home",
"registration":1540919166796.0,
"sessionId":38,
"song":null,
"status":200,
"ts":1541105830796,
"userAgent":"\"Mozilla\/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/36.0.1985.143 Safari\/537.36\"",
"userId":"39"}

Fact Table
1. songplays - records in event data associated with song plays i.e. records with page NextSong
    (songplay_id, 
    start_time, 
    user_id, 
    level, 
    song_id, 
    artist_id, 
    session_id, 
    location, 
    user_agent)

Dimension Tables
2. users - users in the app
    (user_id, first_name, last_name, gender, level)
3. songs - songs in music database
    (song_id, title, artist_id, year, durationy)
4. artists - artists in music database
    (artist_id, name, location, lattitude, longitude)
5. time - timestamps of records in songplays broken down into specific units
    (start_time, hour, day, week, month, year, weekday)
