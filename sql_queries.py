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

staging_events_table_create = (""" 
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
    );""")

staging_songs_table_create = ("""
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
    );""")

# Note that we are ignoring event fields ... auth, iteminSession, length, method, page, registration

songplay_table_create = ("""
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
    );""")

user_table_create = ("""
    CREATE TABLE users (
        user_id INT PRIMARY KEY,
        first_name VARCHAR(256),
        last_name VARCHAR(256),
        gender VARCHAR(64),
        level VARCHAR(64)
    );""")

song_table_create = ("""
    CREATE TABLE songs (
        song_id VARCHAR(30) PRIMARY KEY,
        artist_id VARCHAR, 
        title VARCHAR(512),
        year INT,
        duration REAL
    );""")

artist_table_create = ("""
    CREATE TABLE artists ( 
        artist_id VARCHAR PRIMARY KEY,
        artist_name VARCHAR(512),
        artist_location VARCHAR(512),
        artist_latitude REAL, 
        artist_longitude REAL
    );""")

# values generated from events.ts
time_table_create = ("""
    CREATE TABLE time (  
        start_time BIGINT PRIMARY KEY, 
        hour INT, 
        day INT, 
        week INT, 
        month INT, 
        year INT, 
        weekday INT
    );""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events 
        FROM 's3://udacity-dend/log-data'
        CREDENTIALS 'aws_iam_role={}'
        FORMAT AS JSON 's3://udacity-dend/log_json_path.json'
        compupdate off region 'us-west-2';
""")

staging_songs_copy = ("""
    COPY staging_songs 
        FROM 's3://udacity-dend/song-data/A/A'
        CREDENTIALS 'aws_iam_role={}'
        FORMAT AS JSON 'auto'
        compupdate off region 'us-west-2';
""")

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays ( 
        start_time, user_id, level, song_id,   
        artist_id, session_id, location, user_agent)
    SELECT
        se.ts, se.userId, se.level, ss.song_id,
        ss.artist_id, se.sessionId, se.location, se.userAgent
    FROM staging_events se, staging_songs ss
    WHERE se.song = ss.title and se.artist = ss.artist_name and se.userId is not NULL;
""")

user_table_insert = ("""
    INSERT INTO users (
        user_id, first_name, last_name, gender, level)
    SELECT DISTINCT
        userId, firstName, lastName, gender, level
    FROM staging_events
    WHERE userId is not NULL;
""")

song_table_insert = ("""
    INSERT INTO songs (
        song_id, artist_id, title, year, duration)
    SELECT 
        song_id, artist_id, title, year, duration 
    FROM staging_songs;
""")

artist_table_insert = ("""
    INSERT INTO artists (
        artist_id, artist_name, artist_location, artist_latitude, artist_longitude)
    SELECT DISTINCT
        artist_id, artist_name, artist_location, artist_latitude, artist_longitude
    FROM staging_songs; 
    """)

time_table_insert = (""" 
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
    FROM staging_events e; """)


# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create,
                        user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert,
                        song_table_insert, artist_table_insert, time_table_insert]


# Verification
""" 
ELECT
    COUNT(*) AS total_rows,
    (SELECT COUNT(*) FROM (SELECT DISTINCT user_id, first_name, last_name, gender, level FROM users)) AS unique_rows
FROM 
    users
LIMIT 100;

 
SELECT
    COUNT(*) AS total_rows,
    (SELECT COUNT(*) FROM ( SELECT DISTINCT
        start_time, user_id, level, song_id,   
        artist_id, session_id, location, user_agent from songplays) ) AS unique_rows
FROM 
    songplays
LIMIT 100;

SELECT
    COUNT(*) AS total_rows,
    (SELECT COUNT(*) FROM ( SELECT DISTINCT song_id, artist_id, title, year, duration from songs)) AS unique_rows
FROM  songs
LIMIT 100;

SELECT
    COUNT(*) AS total_rows,
    (SELECT COUNT(*) FROM (SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude from artists))
    AS unique_rows
FROM 
    artists
LIMIT 100;



SELECT
    COUNT(*) AS total_rows,
    (SELECT COUNT(*) FROM (SELECT DISTINCT start_time, hour, day, week, month, year, weekday from time))
    AS unique_rows
FROM 
    time
LIMIT 100; 

"""
