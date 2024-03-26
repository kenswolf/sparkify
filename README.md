# Overview

Sparkify is a music streaming startup with a growing user base and song database.  To obtain a comprehensive understanding for their business, in order to facilitate continued growth, they are moving their data analysis into the cloud.

The project was developed using AWS Serveless Redshift, in order to leverage AWS resource management and reduce costs. AWS Serveless Redshift is the most modern and marketed version of AWS Redshift.

# ETL

The data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs.   This project creates an ETL pipeline that extracts song and song-play data from to staging tables and then transforms and loads the data into a fact-dimension star schema optimized for queries on song play analysis.  An AWS Serverless Redshift is used as the data warehouse. 

The data sources is S3 in the us-west-2 region and the serverless Redshift and its database are in the us-east1-region
 
# Sample Supported Queries

TODO  
* what is the most played song?
* When is the highest usage time of day by hour for songs?
* You could simply have a section of sql_queries.py that is executed after the load is done that prints a question and then the answer.
   

# Data  

The song data is located in s3://udacity-dend/song_data in the us-west-2 region.  It is a subset of real data from the Million Song Dataset.  Each file is in JSON format and contains metadata about a song and the artist of that song. 
The files are partitioned by the first three letters of each song's track ID. (e.g. one song datafile path is song_data/A/B/C/TRABCEI128F424C983.json)
Song datafiles are structured as follows ...  {"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}

The song-play data is located in s3://udacity-dend/log_data in the us-west-2 region. A log_json_path.json files exists with the data, to facilitate loading. 

# Project Execution Steps
 
## Setup AWS Access

1. Log into AWS Console
1. create iam user  with the "AdministratorAccess" policy/privilages 
1. generate secret and key for the new user
1. put the secret and key into dwh.cfg config file 

## Create Infrastructure

1. Run ... python3 iac_create.py ... to  ...

    * create a role that allows redshift to make calls to AWS services and the privilages to read s3
    
    * create a Redshift Serverless 'Namespace' 
    (this auto-magically creates a database, using configuration in the dwh.cfg config file) 
    (new role's ARN is passed in, so the new Redshift Serverless 'Namespace' has the new role)
    
    * create a Redshift Serverless 'Workgroup' 
    which is a group of compute resources.  
    (note a namespace can have multiple workgroups 
    and they can be used selectively as needed,
    but we only need one for this program) 
    
    * create an ingress rule for the IP address of the computer running the iac code  (note ingress rules for additional IP addresses can always be created on AWS console)

Note that this code uses the boto3 library

## Create Database Structure
 
1. Run ... python3 create_tables.py ... to  ...

    * creates the staging database tables and the data warehouse fact-dimension star schema optimized for queries on song play analysis

Note that this code uses the boto3 library.  It does not use the psychopg2 library.  This is simply because I wanted to learn about using the boto3 library for SQL commands.

## Populate the Database
 
1. Run ... python3 etl.py ... to  ...

    * extract from s3 to staging tables
transform and load from staging tables into the data warehouse fact-dimension star schema optimized for queries on song play analysis

Note that this uses the psychopg2 and boto3 librarys

# Support Files
         
* iac_delete.py - Deletes the redshift including the database.  It takes about ten minutes to complete.  It runs until it can verify the deletion or it errors out. Note it does not remove ingress rule because other Redshift may be using it.
* sql_queries.py - Contains all the sql (DDL, inserts, queries)
* boto3_sql_util.py - Includes three boto3 specific sql methods: a generic execute sql method, a sql debugging method, and a method to convert a query result into a pandas DataFrame
* dwh.cfg - Configuration file
* utilities.py - Methods for building printable duration strings, listing files on s3, counting files on s3, downloading files from s3, and retrieving the database host/endpoint.
* sample_queries.py - sample queries of the fact-dimension tables  

# Fact Dimension Schema  
 
## Fact Table

* songplays - records in event data associated with song plays  
songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

## Dimension Tables

* users - users in the app
user_id, first_name, last_name, gender, level

* songs - songs in music database
song_id, title, artist_id, year, duration

* artists - artists in music database
artist_id, name, location, latitude, longitude

* time - timestamps of records in songplays broken down into specific units
start_time, hour, day, week, month, year, weekday

## Complication 

A user's level is in both the users dimention table and songplays fact table.
Some users played songs using both a free status and a paying status.
As a result ...
* the star schema not fully N2 normalized. 
* the unique record key for the user table, and its FK to fact table, is user_id and level, not just the user_id. 
