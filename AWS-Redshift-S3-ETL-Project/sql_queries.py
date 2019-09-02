"""
Description: The groups of queries specified below are to be used to all of
the below.  Data sources are specified within 'dwh.cfg' file.:
- create and drop tables (staging tables and final star schema tables), 
  corresponding to data being loaded from given sources;
- copying data into staging tables into Redshift cluster (staging_events, staging_songs) 
  from S3 bucket provided
- inserting data into final star schema tables in Redshift cluster (songlays, users, songs, 
  artists, time) from staging tables
- defining query lists to be used within programs 'create_tables.py' and 'etl.py'

"""

import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
DWH_ROLE_ARN = config.get('IAM_ROLE','ARN')
LOG_DATA = config.get('S3','LOG_DATA')
LOG_JSONPATH = config.get('S3','LOG_JSONPATH')
SONG_DATA = config.get('S3','SONG_DATA')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_songs_table_create= ("""
                                CREATE TABLE IF NOT EXISTS staging_songs (
                                num_songs INT,
                                artist_id VARCHAR(20),
                                artist_latitude FLOAT4,
                                artist_longitude FLOAT4,
                                artist_location VARCHAR(MAX),
                                artist_name VARCHAR(MAX),
                                song_id VARCHAR(20),
                                title VARCHAR(MAX),
                                duration FLOAT4,
                                year INT
                                );
                                """)

staging_events_table_create = ("""
                                CREATE TABLE IF NOT EXISTS staging_events (
                                artist VARCHAR(MAX), 
                                auth VARCHAR(20), 
                                firstName VARCHAR(25), 
                                gender VARCHAR(2), 
                                itemInSession INT, 
                                lastName VARCHAR(25),
                                length FLOAT4, 
                                level VARCHAR(5), 
                                location VARCHAR(MAX), 
                                method VARCHAR(5), 
                                page VARCHAR(20), 
                                registration BIGINT,
                                sessionId INT, 
                                song VARCHAR(MAX), 
                                status INT, 
                                ts BIGINT, 
                                userAgent VARCHAR(MAX), 
                                userId INT
                                );
                                """)

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (
                            songplay_id INT IDENTITY(0,1), 
                            start_time TIMESTAMP NOT NULL, 
                            user_id INT NOT NULL, 
                            level VARCHAR(5), 
                            song_id VARCHAR(20), 
                            artist_id VARCHAR(20), 
                            session_id INT NOT NULL, 
                            location VARCHAR(MAX), 
                            user_agent VARCHAR(MAX))
                            COMPOUND SORTKEY(song_id, artist_id, user_id);
                         """)

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (
                        user_id INT SORTKEY DISTKEY, 
                        first_name VARCHAR(25), 
                        last_name VARCHAR(25), 
                        gender VARCHAR(2), 
                        level VARCHAR(5));
                     """)

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (
                        song_id VARCHAR(20) SORTKEY DISTKEY, 
                        title VARCHAR(MAX), 
                        artist_id VARCHAR(20), 
                        year INT, 
                        duration FLOAT4);
                     """)

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (
                          artist_id VARCHAR(20) SORTKEY DISTKEY, 
                          name VARCHAR(MAX), 
                          location VARCHAR(MAX), 
                          latitude FLOAT4, 
                          longitude FLOAT4);
                       """)

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (
                        start_time TIMESTAMP SORTKEY DISTKEY, 
                        hour INT, 
                        day INT, 
                        week INT, 
                        month INT, 
                        year INT, 
                        weekday VARCHAR(10));
                     """)

# STAGING TABLES

staging_events_copy = ("""
                          COPY staging_events 
                          FROM '{}'
                          CREDENTIALS 'aws_iam_role={}'
                          FORMAT AS JSON '{}';
                          """).format(LOG_DATA, DWH_ROLE_ARN, LOG_JSONPATH)

staging_songs_copy = ("""
                          COPY staging_songs 
                          FROM '{}'
                          CREDENTIALS 'aws_iam_role={}'
                          FORMAT AS JSON 'auto';
                          """).format(SONG_DATA,DWH_ROLE_ARN)

# FINAL TABLES

songplay_table_insert = ("""
                            INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
                            SELECT timestamp 'epoch' + se.ts/1000 * interval '1 second' AS ts_timestamp, 
                                   se.userId, se.level, ss.song_id, ss.artist_id, se.sessionId, se.location, se.userAgent
                            FROM staging_events se
                                JOIN staging_songs ss
                                ON ss.artist_name = se.artist AND
                                   ss.title = se.song AND
                                   ss.duration = se.length
                            WHERE se.page = 'NextSong'
                            """)

user_table_insert = ("""
                        INSERT INTO users (user_id, first_name, last_name, gender, level)
                        SELECT DISTINCT userId, firstName, lastName, gender, level
                        FROM staging_events
                        WHERE page = 'NextSong'
                        """)

song_table_insert = ("""
                        INSERT INTO songs (song_id, title, artist_id, year, duration)
                        SELECT DISTINCT song_id, title, artist_id, year, duration
                        FROM staging_songs
                        """)

artist_table_insert = ("""
                          INSERT INTO artists (artist_id, name, location, latitude, longitude)
                          SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
                          FROM staging_songs
                          """)

time_table_insert = ("""
                        INSERT INTO time (start_time, hour, day, week, month, year, weekday)
                        SELECT DISTINCT timestamp 'epoch' + ts/1000 * interval '1 second' AS ts_timestamp,
                                        EXTRACT (HOUR FROM ts_timestamp),
                                        EXTRACT (DAY FROM ts_timestamp),
                                        EXTRACT (WEEK FROM ts_timestamp),
                                        EXTRACT (MONTH FROM ts_timestamp),
                                        EXTRACT (YEAR FROM ts_timestamp),
                                        TO_CHAR (ts_timestamp, 'Day')
                        FROM staging_events
                        WHERE page = 'NextSong'
                        """)

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]