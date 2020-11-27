import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE staging_events (
    artist varchar,
    auth varchar,
    first_name varchar,
    last_name varchar
)
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs (
    song_id varchar,
    title varchar,
    artist_id varchar,
    year int,
    duration int
)
""")

songplay_table_create = ("""
CREATE TABLE songplays (
    songplay_id varchar,
    start_time timestamp,
    user_id varchar,
    level int,
    song_id varchar,
    artist_id varchar,
    session_id varchar,
    location varchar,
    user_agent varchar
)
""")

user_table_create = ("""
CREATE TABLE users (
    user_id varchar,
    first_name varchar,
    last_name varchar,
    gender varchar,
    level int
)
""")

song_table_create = ("""
CREATE TABLE songs (
    song_id varchar,
    title varchar,
    artist_id varchar,
    year int,
    duration int
)
""")

artist_table_create = ("""
CREATE TABLE artists (
    artist_id varchar,
    name varchar,
    location varchar,
    latitude float,
    longitude float
)
""")

time_table_create = ("""
CREATE TABLE time (
    start_time timestamp,
    hour int,
    day int,
    week int, 
    month int,
    year int,
    weekday varchar
)
""")

# STAGING TABLES

staging_events_copy = ("""
""").format()

staging_songs_copy = ("""
""").format()

# FINAL TABLES

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
