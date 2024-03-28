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
        FROM '{}'
        CREDENTIALS 'aws_iam_role={}'
        FORMAT AS JSON '{}'
        compupdate off region '{}';
"""

STAGING_SONGS_COPY = """
    COPY staging_songs 
        FROM '{}'
        CREDENTIALS 'aws_iam_role={}'
        FORMAT AS JSON 'auto'
        compupdate off region '{}';
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


# Sample Queries
SAMPLE_QUERY_USERS_WHO_USED_FREE_AND_PAID = """
SELECT  
    u.user_id 
    AS users_who_used_both_free_and_paid
    FROM users u
    WHERE u.user_id in (SELECT s.user_id from users s where s.level = 'free')
    AND u.level = 'paid';"""


SAMPLE_QUERY_TYPES_OF_USERS = """
SELECT
    COUNT(*) AS user_record_count,

    (SELECT COUNT(*) 
    FROM (
        SELECT DISTINCT 
        user_id, first_name, last_name, gender, level  
        FROM users)) 
    AS unique_users_levels_count, 

    (SELECT COUNT(*) 
    FROM (
        SELECT DISTINCT 
        user_id, first_name, last_name, gender
        FROM users)) 
    AS users_count, 
 
    (SELECT COUNT(*) 
    FROM (
        SELECT DISTINCT user_id 
        FROM users
        WHERE level = 'free')) 
    AS free_users_count,

    (SELECT COUNT(*) 
    FROM (
        SELECT DISTINCT user_id 
        FROM users
        WHERE level = 'paid')) 
    AS paid_users_count,

    (SELECT COUNT(*) 
    FROM (
        SELECT DISTINCT u.user_id 
        FROM users u
        WHERE u.level = 'paid'
        AND   u.user_id in  
            (SELECT s.user_id from users s where s.level = 'free'))) 
    AS users_both_free_and_paid_count, 

    (SELECT COUNT(*) 
    FROM (
        SELECT DISTINCT user_id 
        FROM users
        WHERE level != 'free'
        AND   level != 'paid')) 
    AS userds_not_free_and_not_paid_count

FROM users;"""


SAMPLE_QUERY_COUNT_SONGS = "SELECT COUNT(*) FROM songs;"
SAMPLE_QUERY_COUNT_ARTISTS = "SELECT COUNT(*) FROM artists;"

SAMPLE_QUERY_COUNT_SONGPLAYS_BY_USER = """
    SELECT s.user_id, COUNT(*) 
    FROM songplays s, users u 
    WHERE s.user_id = u.user_id 
    GROUP BY s.user_id 
    ORDER BY COUNT(*) DESC;"""

SAMPLE_QUERY_COUNT_SONGPLAYS_BY_YEAR = """
    SELECT t.year, COUNT(*) 
    FROM songplays s, time t 
    WHERE s.start_time = t.start_time 
    GROUP BY t.year 
    ORDER BY t.year ASC;"""

SAMPLE_QUERY_COUNT_SONGPLAYS_BY_DAY = """
    SELECT t.day, COUNT(*) 
    FROM songplays s, time t 
    WHERE s.start_time = t.start_time 
    GROUP BY t.day 
    ORDER BY t.day ASC;"""

SAMPLE_QUERY_COUNT_SONGPLAYS_BY_WEEK = """
    SELECT t.week, COUNT(*) 
    FROM songplays s, time t 
    WHERE s.start_time = t.start_time 
    GROUP BY t.week 
    ORDER BY t.week ASC;"""

SAMPLE_QUERY_COUNT_SONGPLAYS_BY_MONTH = """
    SELECT t.month, COUNT(*) 
    FROM songplays s, time t 
    WHERE s.start_time = t.start_time 
    GROUP BY t.month 
    ORDER BY t.month ASC;"""

SAMPLE_QUERY_COUNT_SONGPLAYS_BY_HOUR = """
    SELECT t.hour, COUNT(*) 
    FROM songplays s, time t 
    WHERE s.start_time = t.start_time 
    GROUP BY t.hour 
    ORDER BY t.hour ASC;"""

SAMPLE_QUERY_COUNT_SONGPLAYS_BY_WEEKDAY = """
    SELECT t.weekday, COUNT(*) 
    FROM songplays s, time t 
    WHERE s.start_time = t.start_time 
    GROUP BY t.weekday 
    ORDER BY t.weekday ASC;"""


SAMPLE_QUERY_POPULAR_ARTISTS = """
    SELECT count(*), a.artist_name  
    FROM songplays s, artists a 
    WHERE s.artist_id = a.artist_id 
    GROUP BY a.artist_name 
    ORDER BY COUNT(*) DESC;"""

SAMPLE_QUERY_POPULAR_SONGS = """
    SELECT count(*), g.title
    FROM songplays s, songs g 
    WHERE s.song_id = g.song_id 
    GROUP BY g.title
    ORDER BY COUNT(*) DESC;"""


sample_queries = [SAMPLE_QUERY_USERS_WHO_USED_FREE_AND_PAID,
                  SAMPLE_QUERY_TYPES_OF_USERS,
                  SAMPLE_QUERY_COUNT_SONGS,
                  SAMPLE_QUERY_COUNT_ARTISTS,
                  SAMPLE_QUERY_COUNT_SONGPLAYS_BY_USER,
                  SAMPLE_QUERY_COUNT_SONGPLAYS_BY_YEAR,
                  SAMPLE_QUERY_COUNT_SONGPLAYS_BY_DAY,
                  SAMPLE_QUERY_COUNT_SONGPLAYS_BY_WEEK,
                  SAMPLE_QUERY_COUNT_SONGPLAYS_BY_MONTH,
                  SAMPLE_QUERY_COUNT_SONGPLAYS_BY_HOUR,
                  SAMPLE_QUERY_COUNT_SONGPLAYS_BY_WEEKDAY,
                  SAMPLE_QUERY_POPULAR_ARTISTS,
                  SAMPLE_QUERY_POPULAR_SONGS]

sample_query_titles = ['Id of Users who played songs for free and as paid',
                       'Number of Users, of different types',
                       'Number of Songs',
                       'Number of Artists',
                       'Users who played the most songs',
                       'Song play volume by year',
                       'Song play volume by day of the month',
                       'Song play volume by week of the year',
                       'Song play volume by month',
                       'Song play volume by hour',
                       'Song play volume by weekday',
                       'Most played Artists',
                       'Most played Songs']
