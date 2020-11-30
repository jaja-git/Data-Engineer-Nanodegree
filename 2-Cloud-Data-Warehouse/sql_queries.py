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
    artist text,
    auth text,
    first_name text,
    gender text,
    item_in_session int,
    last_name text,
    length decimal,
    level text,
    location text,
    method text,
    page text,
    registration decimal,
    session_id int,
    song text,
    status int,
    ts bigint,
    user_agent text,
    user_id int
)
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs (
    num_songs int,
    artist_id text,
    artist_latitude numeric,
    artist_longitude numeric,
    artist_location text,
    artist_name text,
    song_id text,
    title text,
    duration numeric
)
""")

songplay_table_create = ("""
CREATE TABLE songplays (
    songplay_id text,
    start_time timestamp,
    user_id text,
    level int,
    song_id text,
    artist_id text,
    session_id text,
    location text,
    user_agent text
)
""")

user_table_create = ("""
CREATE TABLE users (
    user_id text,
    first_name text,
    last_name text,
    gender text,
    level int
)
""")

song_table_create = ("""
CREATE TABLE songs (
    song_id text,
    title text,
    artist_id text,
    year int,
    duration int
)
""")

artist_table_create = ("""
CREATE TABLE artists (
    artist_id text,
    name text,
    location text,
    latitude numeric,
    longitude numeric
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
    weekday text
)
""")

# STAGING TABLES

staging_events_copy = ("""
                        COPY staging_events
                        FROM {}
                        credentials 'aws_iam_role={}'
                        json 'auto'
                        EMPTYASNULL
                        BLANKSASNULL
                    """).format(config['S3']['LOG_DATA'], 
                                config['IAM_ROLE']['ARN'])


staging_songs_copy = ("""
                     COPY staging_events 
                     FROM {}
                     credentials 'aws_iam_role={}'
                     json 'auto'
                     EMPTYASNULL
                     BLANKSASNULL
                    """).format(config['S3']['SONG_DATA'], 
                                config['IAM_ROLE']['ARN'])

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
