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
    length numeric,
    level text,
    location text,
    method text,
    page text,
    registration numeric,
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
    artist_id text,
    artist_latitude numeric,
    artist_location text,    
    artist_longitude numeric,
    artist_name text,
    duration numeric,
    num_songs int,
    song_id text,
    title text,
    year int
)
""")

songplay_table_create = ("""
CREATE TABLE songplays (
    songplay_id text PRIMARY KEY,
    start_time timestamp,
    user_id text,
    level text,
    song_id text,
    artist_id text,
    session_id text,
    location text,
    user_agent text
)
""")

user_table_create = ("""
CREATE TABLE users (
    user_id text PRIMARY KEY,
    first_name text,
    last_name text,
    gender text,
    level text
)
""")

song_table_create = ("""
CREATE TABLE songs (
    song_id text PRIMARY KEY,
    title text,
    artist_id text,
    year int,
    duration int
)
""")

artist_table_create = ("""
CREATE TABLE artists (
    artist_id text PRIMARY KEY,
    name text,
    location text,
    latitude numeric,
    longitude numeric
)
""")

time_table_create = ("""
CREATE TABLE time (
    start_time timestamp PRIMARY KEY,
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
                        copy staging_events
                        from {}
                        credentials 'aws_iam_role={}'
                        json {}
                        emptyasnull
                        blanksasnull
                    """).format(config['S3']['LOG_DATA'], 
                                config['IAM_ROLE']['ARN'],
                                config['S3']['LOG_JSONPATH'])


staging_songs_copy = ("""
                     copy staging_songs
                     from {}
                     credentials 'aws_iam_role={}'
                     json 'auto'
                     emptyasnull
                     blanksasnull
                     truncatecolumns
                 """).format(config['S3']['SONG_DATA'], 
                                config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' as start_time,
 user_id, level::text, song_id, artist_id, session_id, location, user_agent
  from staging_events e
  LEFT JOIN staging_songs s
  on e.song = s.title and e.artist = s.artist_name
WHERE e.page = 'NextPage'
AND start_time IS NOT NULL
AND user_id IS NOT NULL
AND song_id IS NOT NULL
AND artist_id IS NOT NULL 
and page = 'NextPage';
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT user_id, first_name, last_name, gender, level::text from staging_events
WHERE user_id IS NOT NULL
AND first_name IS NOT NULL
AND last_name IS NOT NULL
and page = 'NextPage'
;
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id,  year, duration)
SELECT song_id, title, artist_id, year, duration from staging_songs
WHERE song_id IS NOT NULL
AND artist_id IS NOT NULL
AND title IS NOT NULL
and page = 'NextPage';
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT artist_id, artist_name, artist_location, artist_latitude, artist_longitude from staging_songs
WHERE artist_id IS NOT NULL
AND artist_name IS NOT NULL
;
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
WITH sq as (SELECT TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' as ts FROM staging_events) 
SELECT ts as start_time, 
       date_part('hour', ts) as hour,
       date_part('day', ts) as day,
       date_part('week', ts) as week,
       date_part('month', ts) as month,
       date_part('year', ts) as year,
       date_part('weekday', ts) as weekday  
FROM sq
WHERE start_time IS NOT NULL
and page = 'NextPage';
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_songs_copy, staging_events_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
