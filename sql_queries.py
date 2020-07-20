import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_songs;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_events;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE "staging_events"(
    "artist" VARCHAR,
    "auth" VARCHAR,
    "firstName" VARCHAR(50),
    "gender" CHAR,
    "itemInSession" INTEGER,
    "lastName" VARCHAR(50),
    "length" FLOAT,
    "level" VARCHAR,
    "location" VARCHAR,
    "method" VARCHAR,
    "page" VARCHAR,
    "registration" FLOAT,
    "sessionId" INTEGER,
    "song" VARCHAR,
    "status" INTEGER,
    "ts" BIGINT,
    "userAgent" VARCHAR,
    "userId" INTEGER);
""")

staging_songs_table_create = ("""
CREATE TABLE "staging_songs"(
    "num_songs" INTEGER,
    "artist_id" VARCHAR,
    "artist_latitude" FLOAT,
    "artist_longitude" FLOAT,
    "artist_location" VARCHAR,
    "artist_name" VARCHAR,
    "song_id" VARCHAR,
    "title" VARCHAR,
    "duration" FLOAT,
    "year" FLOAT    
    );
""")

songplay_table_create = ("""
CREATE TABLE "songplays"(
    "songplay_id" INTEGER IDENTITY (1, 1) PRIMARY KEY ,
    "start_time" TIMESTAMP distkey sortkey,
    "user_id" INTEGER not null,
    "level" VARCHAR,
    "song_id" VARCHAR not null,
    "artist_id" VARCHAR not null,
    "session_id" INTEGER,
    "location" VARCHAR,
    "user_agent" VARCHAR
    );
""")

user_table_create = ("""
CREATE TABLE "users"(
    "user_id" INTEGER PRIMARY KEY distkey sortkey,
    "first_name" VARCHAR,
    "last_name" VARCHAR,
    "gender" VARCHAR,
    "level"  VARCHAR 
    );
""")

song_table_create = ("""
CREATE TABLE "songs"(
    "song_id" VARCHAR PRIMARY KEY distkey sortkey,
    "title" VARCHAR,
    "artist_id" VARCHAR not null,
    "year" INTEGER,
    "duration"  FLOAT 
    );
""")

artist_table_create = ("""
CREATE TABLE "artists"(
    "artist_id" VARCHAR PRIMARY KEY distkey sortkey,
    "name" VARCHAR,
    "location" VARCHAR,
    "latitude" FLOAT,
    "longitude"  FLOAT 
    );
""")

time_table_create = ("""
CREATE TABLE "time"(
    "start_time" TIMESTAMP PRIMARY KEY distkey sortkey,
    "hour" INTEGER,
    "day" INTEGER,
    "week" INTEGER,
    "month"  INTEGER,
    "year"  INTEGER,
    "weekday" VARCHAR
    );
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events
FROM {}
credentials 'aws_iam_role={}'
FORMAT AS json {};
""").format(config['S3']['LOG_DATA'], config['CLUSTER']['DWH_ROLE_ARN'],config['S3']['LOG_JSONPATH'])
                       
staging_songs_copy = ("""
COPY staging_songs FROM 's3://udacity-dend/song_data'
    credentials 'aws_iam_role={}'
    FORMAT AS json 'auto'; 
""").format(config['CLUSTER']['DWH_ROLE_ARN'])


# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, 
                       location, user_agent)
SELECT DISTINCT(TIMESTAMP 'epoch' + (ev.ts / 1000) * INTERVAL '1 second') as start_time,
                ev.userId,
                ev.level,
                so.song_id,
                so.artist_id,
                ev.sessionId,
                ev.location,
                ev.userAgent
FROM stagine_songs so
INNER JOIN staging_events ev
ON (so.title = ev.song AND ev.artist = so.artist_name)
AND ev.page = 'NextSong';
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT userId, firstName, lastName, gender, level
FROM staging_events
WHERE userId IS NOT NULL
AND page = 'NextSong'
on conflict (user_id) do update SET level = EXCLUDED.level;
""")

song_table_insert = ("""
INSERT INTO songs
SELECT DISTINCT song_id, title, artist_id, year, duration
FROM stagine_songs
WHERE song_id IS NOT NULL
on conflict (song_id) do nothing;
""")

artist_table_insert = ("""
INSERT INTO artists
SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
FROM stagine_songs
on conflict (artist_id) do nothing; 
""")

time_table_insert = ("""
INSERT into time
SELECT DISTINCT
       TIMESTAMP 'epoch' + (ts/1000) * INTERVAL '1 second' as start_time,
       EXTRACT(HOUR FROM start_time) AS hour,
       EXTRACT(DAY FROM start_time) AS day,
       EXTRACT(WEEKS FROM start_time) AS week,
       EXTRACT(MONTH FROM start_time) AS month,
       EXTRACT(YEAR FROM start_time) AS year,
       to_char(start_time, 'Day') AS weekday
FROM staging_events
on conflict (ts) do nothing;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy]
#[staging_events_copy, staging_songs_copy]

insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
