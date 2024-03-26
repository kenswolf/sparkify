# Overview

    Sparkify is a music streaming startup with a growing user base and song database. 

    To obtain a comprehensive understanding for their business, in order to facilitate continued growth, they are moving their data analysis into the cloud.

# ETL

    Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app. 

    This project includes an ETL pipeline that extracts song and song-play data from to staging tables in Redshift, and then transforms the data into a fact-dimension star schema optimized for queries on song play analysis. 

# Regions

    The data sources is S3 in the us-west-2 region.

    The serverless Redshift and its database are in the us-east1-region


# Sample Supported Queries

    TODO

    Provide example queries and results for song play analysis. We do not provide you any of these. You, as part of the Data Engineering team were tasked to build this ETL. Thorough study has gone into the star schema, tables, and columns required. The ETL will be effective and provide the data and in the format required. However, as an exercise, it seems almost silly to NOT show SOME examples of potential queries that could be ran by the users. PLEASE use your imagination here. For example, what is the most played song? When is the highest usage time of day by hour for songs? It would not take much to imagine what types of questions that corporate users of the system would find interesting. Including those queries and the answers makes your project far more compelling when using it as an example of your work to people / companies that would be interested. You could simply have a section of sql_queries.py that is executed after the load is done that prints a question and then the answer.
    Example Output From An ETL Run

# Song Data  

    The data is located here ... s3://udacity-dend/song_data  ... in the us-west-2 region. 
    Song Dataset is a subset of real data from the Million Song Dataset. 
    Each file is in JSON format and contains metadata about a song and the artist of that song. 
    The files are partitioned by the first three letters of each song's track ID. (e.g. one song datafile path is song_data/A/B/C/TRABCEI128F424C983.json)
    Song datafiles are structured like this ...  {"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}

# Song-Plan Data

    The data is located here ... s3://udacity-dend/log_data ... in the us-west-2 region. 
    To properly read log data s3://udacity-dend/log_data, you'll need the following metadata file: 
    Log metadata: s3://udacity-dend/log_json_path.json (also in the us-west-2 region) 

# Project Execution Steps
 
    ### Setup AWS Access

        ##### Log into AWS Console
        ##### create iam user  with the "AdministratorAccess" policy/privilages 
        ##### generate secret and key for the new user
        ##### put the secret and key into dwh.cfg config file 

    ### Create Infrastructure

        ##### Run ... python3 iac.py ... to  ...
            
            create a role that allows redshift to make calls to AWS services and the privilages to read s3

            create a Redshift Serverless 'Namespace' 
                (this auto-magically creates a database, using configuration in the dwh.cfg config file) 
                (new role's ARN is passed in, so the new Redshift Serverless 'Namespace' has the new role)

            create a Redshift Serverless 'Workgroup' 
                which is a group of compute resources.  
                (note a namespace can have multiple workgroups 
                and they can be used selectively as needed,
                but we only need one for this program) """

        ##### Note that this code uses the boto3 library
  
    ### Create Database Structure

        ##### Run create_tables (ie python3 create_tables.py) to ...
    
        ##### creates the staging database tables and the data warehouse fact-dimension star schema optimized for queries on song play analysis

        ##### Note that this code uses the boto3 library
  
    ### Populate the Database
    
        ##### Run etl.py (ie python3 create_tables.py) to ...

        extract from s3 to staging tables
        transform and load from staging tables into the data warehouse fact-dimension star schema optimized for queries on song play analysis



# Support Files
         
    sql_queries.py - contains all the sql (DDL, inserts, queries)
    boto3_sql_util.py - includes three boto3 specific methods: a generic execute sql method, a sql debugging method, and a method to convert a query result into a pandas DataFrame
    dwh.cfg - configuration file
    util_s3.py - methods related to s3 such as security policy creation, query, and download
 

# TODO
    execute SQL statements that create the analytics tables from these staging tables.  
    Use IAC to implement the destruction of infrastructure.   
    cleanup code
    polish  

# Fact Dimension Tables 
   
 
### Fact Table

    ##### songplays
    
    records in event data associated with song plays i.e. records with page NextSong
    songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

### Dimension Tables

    ##### Notes
    A user's level is in both the users dimention table and songplays fact table.
    As a result users who played songs using both a free status and a paying status,
    are in the user table twice.  This makes the star schema not  N2 normalized.
    Also, a consiquence of this is that the FK to connect the songplay and user tables 
    is the user_id and level, not just the user_id.

    ##### users - users in the app
    user_id, first_name, last_name, gender, level

    ##### songs - songs in music database
    song_id, title, artist_id, year, duration

    ##### artists - artists in music database
    artist_id, name, location, latitude, longitude

    ##### time - timestamps of records in songplays broken down into specific units
    start_time, hour, day, week, month, year, weekday
 
# Quality Control 

    Used Query Editor in the AWS Redshift console to verify the ETL Pipeline 
    Ran the analytic queries o compare your results with the expected results.
    Delete test serverless redshift when finished.

 
 
