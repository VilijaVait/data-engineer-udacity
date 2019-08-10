## Project
Udacity Data Engineer Nanodegree - Data Modeling with Postgres


## Purpose

Sparkify, a startup alloing users to play their favourite music anytime and anywher, wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to.

## Database solution

The Postgres database has star schema, where facts table focuses on song play instances, and dimensions tables provide details of various song/user dimensions.  In particular:

- Facts table ('songplays') contains:
  songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
  
- Dimension table are and contain:
  - users (*user_id, first_name, last_name, gender, level*),
  - songs (*song_id, title, artist_id, year, duration*)
  - artists (*artist_id, name, location, latitude, longitude*)
  - time (*start_time, hour, day, week, month, year, weekday*)
  
This schema allows for queries related to who/what/when is listening to the songs, for example:
- what is the average/total listening duration of all free vs paid users last week/day/hour
- what top 5 songs where played and how many times in last week/day/hour
- what top 5 artists where played and how many times in last week/day/hour
- what is the average/total listening duration during each day of the week/time of day

Such information could allow the company to optimise their song inventory, data loads, marketing efforts (focus) among others.

## Use

Run create_tables.py to create the database and tables within the schema, and to drop them (for refresh).

Run etl.py to extract, transform and load data to the tables.

Data sources should reside in specified path ways:  "data/song_data" and "data/log_data" in a specific format (for which the ETL process was subsequently designed for).

"create_tables.py" and "etl.py" use SQL queries defined within "sql_queries.py" file.

## Test

Run "test.ipynb" to test the ETL process. 