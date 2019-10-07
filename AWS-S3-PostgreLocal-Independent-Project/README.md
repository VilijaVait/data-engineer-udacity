## Project
Facilitates data extraction CSV files from S3 into Postgres local instance, via Pandas.  Possible to also add transformation in Pandas in between. Here there are only checks to ensure data was extracted as expected before loading, and transformation to one of the tables (where data contained unexpected duplicates).

## Purpose

This simple ETL process is useful if you want to analyse small to medium size datasets available on various public S3 buckets without incurring any costs on your local Postgres server (costs would be incurred if using Amazon RDS, EMR, Redshift or other services for analysis).

I use here data from Sparkify, a startup allowing users to play their favourite music anytime and anywhere.  This is a mock startup and data provided by Udacity for Data Analytics/Data Engineer nanodegrees.  The data in S3 is in my private S3 bucket.  You can however, adopt it to any data within public S3 buckets. 

## Solution


### Stack

The connection with local Postgres is established,and a new schema (ie relevant tables) is created, using `psycopg2` library. The queries for creation of tables (and dropping, if required) are written in a file `sql_queries.py`.

The data is first extracted from public S3 bucket, where it is expected that each folder (i.e. prefix) will contain data for each table in a schema.  The connection with S3 is established using `boto3`, and `configparser` and `os` for defining environmental variables required for AWS authentication.  You will need your AWS access key and secret key for this.

The data is extracted into Pandas, this is enabled by `s3fs` library. Once the library is installed, you can simply use `read_csv` method with S3 filepath to read data into Pandas dataframe.

I keep each dataframe related to each table in a dictionary, where key:value pair is 'table name': 'table data' (string:dataframe) respectively.

There is simple examination of each dataframe in a dicionary for general accuracy (shape, data types etc).  There are also some transformations to `artist` table, due to existence of unexpected duplicates there (no rows were duplicated, but some (artist_id, name) pairs were duplicated, and I need to keep this as unique pairs- primary keys- in target table). 

Then load each respective dataframe (within the dictionary) into a Postgres table using connection established with `sqlalchemy`.  Because the loading method used `to_sql` is a Pandas method, in order to establish connection with database, you need to use sqlalchemy.engine.Engine or sqlite3.Connection, as defined in Pandas methods [https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.to_sql.html].

The tables should be created before loading (in this case, at the beginning), to ensure desired data types.  The Pandas method also specifies what happens if table does not exist (use if_exists='replace' or 'fail'), but I would not trust Pandas to define your data types for you.

Then test if tables were loaded correctly.


### Design

The design for the particular schema used in the example is as follows:

- Facts table ('songplays') contains:
  songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
  
- Dimension table are and contain:
  - users (*user_id, first_name, last_name, gender, level*),
  - songs (*song_id, title, artist_id, year, duration*)
  - artists (*artist_id, name, location, latitude, longitude*)
  - time (*start_time, hour, day, week, month, year, weekday*)
  

## Data sources
Data source could be any public S3 buckets (ideally with separate folders for each table).  Or you can extract data from non-public S3 buckets, to which you have access to with your credentials.

In this case, I used S3 data source that are my own private S3 buckets, derived from Udacity data for Sparkify music company (which is available in public buckets at the moment, but in different, log, format). 

## Use

1. Populate "db.cfg" file with AWS connnection credentials.
2. Make sure to insert your own S3 bucket, prefix details within approapriate space in `ETL from S3 to Postgres local` notebook; specify your own Postgre database and table details; as well as amend `sql_queries` to reflect tables that you need to create and then load data into.
3. Run all steps specified in `ETL from S3 to Postgres local` notebook.