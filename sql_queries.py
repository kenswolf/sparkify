"""" This file contains all the SQL used in this project """
import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

STAGING_EVENTS_TABLE_DROP = "DROP TABLE IF EXISTS staging_events;"
STAGING_SONGS_TABLE_DROP = "DROP TABLE IF EXISTS staging_songs;"

SONGPLAYS_TABLE_DROP = "DROP TABLE IF EXISTS songplays;"
USERS_TABLE_DROP = "DROP TABLE IF EXISTS users;"
SONGS_TABLE_DROP = "DROP TABLE IF EXISTS songs;"
ARTISTS_TABLE_DROP = "DROP TABLE IF EXISTS artists;"
TIME_TABLE_DROP = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

STAGING_EVENTS_TABLE_CREATE = """
    CREATE TABLE staging_events (
        artist VARCHAR(256),
        auth VARCHAR(64),
        firstName VARCHAR(256),
        gender VARCHAR(64),
        iteminSession INT,
        lastName VARCHAR(256),
        length REAL,
        level VARCHAR(64),
        location VARCHAR(512),
        method VARCHAR(64),
        page VARCHAR(128),
        registration BIGINT,
        sessionId INT,
        song VARCHAR(512),
        status INT,
        ts BIGINT,
        userAgent VARCHAR(512),
        userId INT
    );"""

STAGING_SONGS_TABLE_CREATE = """
    CREATE TABLE staging_songs (
        song_id VARCHAR(64) PRIMARY KEY,
        num_songs INT,
        title VARCHAR(512),
        artist_name VARCHAR(512),
        artist_latitude REAL, 
        year INT,
        duration REAL,
        artist_id VARCHAR(64),
        artist_longitude REAL,
        artist_location VARCHAR(512) 
    );"""

# Note that we are ignoring event fields ... auth, iteminSession, length, method, page, registration

SONGPLAYS_TABLE_CREATE = """
    CREATE TABLE songplays (
        songplay_id INT PRIMARY KEY IDENTITY(0,1),  
        start_time BIGINT, 
        user_id INT,
        level VARCHAR(64),    
        song_id VARCHAR(64),
        artist_id VARCHAR(64),
        session_id INT,
        location VARCHAR(512),
        user_agent VARCHAR(512)
    );"""

USERS_TABLE_CREATE = """
    CREATE TABLE users (
        user_id INT PRIMARY KEY,
        first_name VARCHAR(256),
        last_name VARCHAR(256),
        gender VARCHAR(64),
        level VARCHAR(64)
    );"""

SONGS_TABLE_CREATE = """
    CREATE TABLE songs (
        song_id VARCHAR(30) PRIMARY KEY,
        artist_id VARCHAR, 
        title VARCHAR(512),
        year INT,
        duration REAL
    );"""

ARTISTS_TABLE_CREATE = """
    CREATE TABLE artists ( 
        artist_id VARCHAR PRIMARY KEY,
        artist_name VARCHAR(512),
        artist_location VARCHAR(512),
        artist_latitude REAL, 
        artist_longitude REAL
    );"""

# values generated from events.ts
TIME_TABLE_CREATE = """
    CREATE TABLE time (  
        start_time BIGINT PRIMARY KEY, 
        hour INT, 
        day INT, 
        week INT, 
        month INT, 
        year INT, 
        weekday INT
    );"""

# STAGING TABLES

STAGING_EVENTS_COPY = """
    COPY staging_events 
        FROM 's3://udacity-dend/log-data'
        CREDENTIALS 'aws_iam_role={}'
        FORMAT AS JSON 's3://udacity-dend/log_json_path.json'
        compupdate off region 'us-west-2';
"""

STAGING_SONGS_COPY = """
    COPY staging_songs 
        FROM 's3://udacity-dend/song-data/A/A'
        CREDENTIALS 'aws_iam_role={}'
        FORMAT AS JSON 'auto'
        compupdate off region 'us-west-2';
"""

# FINAL TABLES

SONGPLAYS_TABLE_INSERT = """
    INSERT INTO songplays ( 
        start_time, user_id, level, song_id,   
        artist_id, session_id, location, user_agent)
    SELECT
        se.ts, se.userId, se.level, ss.song_id,
        ss.artist_id, se.sessionId, se.location, se.userAgent
    FROM staging_events se, staging_songs ss
    WHERE se.song = ss.title and se.artist = ss.artist_name and se.userId is not NULL;
"""

USERS_TABLE_INSERT = """
    INSERT INTO users (
        user_id, first_name, last_name, gender, level)
    SELECT DISTINCT
        userId, firstName, lastName, gender, level
    FROM staging_events
    WHERE userId is not NULL;
"""

SONGS_TABLE_INSERT = """
    INSERT INTO songs (
        song_id, artist_id, title, year, duration)
    SELECT 
        song_id, artist_id, title, year, duration 
    FROM staging_songs;
"""

ARTISTS_TABLE_INSERT = """
    INSERT INTO artists (
        artist_id, artist_name, artist_location, artist_latitude, artist_longitude)
    SELECT DISTINCT
        artist_id, artist_name, artist_location, artist_latitude, artist_longitude
    FROM staging_songs; 
    """

TIME_TABLE_INSERT = """
    INSERT INTO time (
        start_time, hour, day, week, month, year, weekday) 
    SELECT DISTINCT
        e.ts,
        EXTRACT(HOUR    from TIMESTAMP 'epoch' + (e.ts / 1000) * INTERVAL '1 second') ,
        EXTRACT(DAY     from TIMESTAMP 'epoch' + (e.ts / 1000) * INTERVAL '1 second') ,
        EXTRACT(WEEK    from TIMESTAMP 'epoch' + (e.ts / 1000) * INTERVAL '1 second') ,
        EXTRACT(MONTH   from TIMESTAMP 'epoch' + (e.ts / 1000) * INTERVAL '1 second') ,
        EXTRACT(YEAR    from TIMESTAMP 'epoch' + (e.ts / 1000) * INTERVAL '1 second') ,
        EXTRACT(WEEKDAY from TIMESTAMP 'epoch' + (e.ts / 1000) * INTERVAL '1 second')  
    FROM staging_events e; """


# QUERY LISTS

create_table_queries = [STAGING_EVENTS_TABLE_CREATE,
                        STAGING_SONGS_TABLE_CREATE,
                        SONGPLAYS_TABLE_CREATE,
                        USERS_TABLE_CREATE,
                        SONGS_TABLE_CREATE,
                        ARTISTS_TABLE_CREATE,
                        TIME_TABLE_CREATE]

drop_table_queries = [STAGING_EVENTS_TABLE_DROP,
                      STAGING_SONGS_TABLE_DROP,
                      SONGPLAYS_TABLE_DROP,
                      USERS_TABLE_DROP,
                      SONGS_TABLE_DROP,
                      ARTISTS_TABLE_DROP,
                      TIME_TABLE_DROP]

copy_table_queries = [STAGING_EVENTS_COPY,
                      STAGING_SONGS_COPY]

insert_table_queries = [SONGPLAYS_TABLE_INSERT,
                        USERS_TABLE_INSERT,
                        SONGS_TABLE_INSERT,
                        ARTISTS_TABLE_INSERT,
                        TIME_TABLE_INSERT]


# Verification
""" 
SELECT
    COUNT(*) AS total_rows,
    (SELECT COUNT(*) FROM 
    (SELECT DISTINCT user_id, first_name, 
        last_name, gender, level FROM users)) 
    AS unique_rows
FROM 
    users
LIMIT 100;

 
SELECT
    COUNT(*) AS total_rows,
    (SELECT COUNT(*) FROM ( SELECT DISTINCT
        start_time, user_id, level, song_id,   
        artist_id, session_id, location, 
        user_agent from songplays) ) 
    AS unique_rows
FROM 
    songplays
LIMIT 100;

SELECT
    COUNT(*) AS total_rows,
    (SELECT COUNT(*) FROM 
    ( SELECT DISTINCT song_id, 
        artist_id, title, year, 
        duration from songs)) 
    AS unique_rows
FROM  songs
LIMIT 100;

SELECT
    COUNT(*) AS total_rows,
    (SELECT COUNT(*) 
    FROM 
        (SELECT DISTINCT artist_id, artist_name, 
        artist_location, artist_latitude, 
        artist_longitude from artists))
    AS unique_rows
FROM 
    artists
LIMIT 100;



SELECT
    COUNT(*) AS total_rows,
    (SELECT COUNT(*) 
    FROM (SELECT DISTINCT start_time, 
        hour, day, week, month, 
        year, weekday from time))
    AS unique_rows
FROM 
    time
LIMIT 100; 

"""
