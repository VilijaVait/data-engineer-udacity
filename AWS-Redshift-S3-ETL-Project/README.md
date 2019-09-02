## Project
Udacity Data Engineer Nanodegree - Data Warehouse, ETL in AWS S3, Redshift


## Purpose

Sparkify, a startup allowing users to play their favourite music anytime and anywhere, has grown their user base and song database and wants to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

## Database solution

### Design

The final database design is star schema, where facts table focuses on song play instances, and dimensions tables provide details of various song/user dimensions.  In particular:

- Facts table ('songplays') contains:
  songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
  
- Dimension table are and contain:
  - users (*user_id, first_name, last_name, gender, level*),
  - songs (*song_id, title, artist_id, year, duration*)
  - artists (*artist_id, name, location, latitude, longitude*)
  - time (*start_time, hour, day, week, month, year, weekday*)
  
Such information could allow the company to optimise their song inventory, data loads, marketing efforts (focus) among others.

### Stack

The data is first copyed into Redshift from S3, in its original form, into staging tables `staging_events` and `staging_songs`.  Then the relevant data, after filtering, is inserted into final star schema tables. Data filters only include events that are 'song-plays' and, for dimension tables, duplicated rows are excluded.

## Data sources
Data sources reside in S3 buckets specified:
LOG_DATA in s3://udacity-dend/log_data
LOG_JSONPATH in s3://udacity-dend/log_json_path.json
SONG_DATA in s3://udacity-dend/song_data

## Use

1. Populate "dwh.cfg" file with Redshift connnection credentials (make sure it's an active cluster).
2. Run create_tables.py to create the database and tables within the schema, and to drop them (for refresh).
3. Run etl.py to load data into staging tables, and then to load data from staging to final tables.
4. "create_tables.py" and "etl.py" use SQL queries defined within "sql_queries.py" file.

## Test

Use "test.ipynb" to test sample queries, provided within the notebook.  