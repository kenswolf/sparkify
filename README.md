# Overview

Sparkify is a music streaming startup with a growing user base and song database.  To obtain a comprehensive understanding for their business, in order to facilitate continued growth, they are moving their data analysis into the cloud.

The project was developed using AWS Serveless Redshift, in order to leverage AWS resource management and reduce costs. AWS Serveless Redshift is the most modern and marketed version of AWS Redshift.

# ETL

The data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs.   This project creates an ETL pipeline that extracts song and song-play data from to staging tables and then transforms and loads the data into a fact-dimension star schema optimized for queries on song play analysis.  An AWS Serverless Redshift is used as the data warehouse. 

The data sources is S3 in the us-west-2 region and the serverless Redshift and its database are in the us-east1-region
 
# Sample Supported Queries

These are the queries run by analytics.py in step six (see below).

## Queries for understanding the user community

* Id of Users who played songs for free and as paid
* Types of Users
* Users who played the most songs

## Queries for understanding the bredth of the catalog
* Number of Songs
* Number of Artists 
* Most played Artists
* Most played Songs

## Queries for understanding play volume
* Song play volume by year
* Song play volume by day of the month
* Song play volume by week of the year
* Song play volume by month
* Song play volume by weekday 

# Data  

The song data is located in s3://udacity-dend/song_data in the us-west-2 region.  It is a subset of real data from the Million Song Dataset.  Each file is in JSON format and contains metadata about a song and the artist of that song. 
The files are partitioned by the first three letters of each song's track ID. (e.g. one song datafile path is song_data/A/B/C/TRABCEI128F424C983.json)
Song datafiles are structured as follows ...  {"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}

The song-play data is located in s3://udacity-dend/log_data in the us-west-2 region. A log_json_path.json files exists with the data, to facilitate loading. 

# Project Assessment Steps

The assigment for this project, in the Udacity Data Engineering with AWS nano-degree, did not include all the functionality in this project.  

For example using 3rd party boto3 and requests libraries, a serverless Redshift instead of a clustered Redshift, and the infrastructure as code, is all above and beyond the assignment.  

As a result the entire project can be run with all the bells and whistles, or just create_tables.py and etl.py can be run on an existing Redshift cluster.

As a result in order to assess what was specifically assigned, the Udacity reviewer will follow these the steps:

   1. There will be an already running Redshift cluster (created by reviewer)
   1. The reviewer will update the config file (dwh.cfg) with cluster identifier (key is 'host'), IAM ROLE ARN (key is 'arn') and DB credentials for the above mentioned cluster
   1. Runs the create_tables.py and etl.py scripts
   1. Check the final tables.
 

# Full Project Execution Steps

## Step 1 - Setup Python Libraries

This can be done two ways ...
* Installying required libraries from a provided configuration file
   * pip install -r requirements.txt 
* Installying required libraries individually 
   1. pip install --upgrade pip
   2. pip install boto3
   4. pip install requests 
   3. pip install prettytable
   5. pip install psycopg2 
 
## Step 2 - Setup AWS Access

1. Log into AWS Console
1. create iam user  with the "AdministratorAccess" policy/privilages 
1. generate secret and key for the new user
1. put the secret and key into dwh.cfg config file 
   (as the values of KEY and SECRET in the AWS section of the configuration file)

## Step 3 - Create Infrastructure

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

    * write the host endpoint and the role arn to the config file 
      1. This is done to seperate the boto3/aws-iac code, 
        that determines and writes the host and role arn values to code, from the sql client code, 
        that creates and populated db tables which needs the host and role arn values
      1. This is done because this project does a lot more than the assignment for which is
        actually graded, and it is graded using an existing reshift cluster and only
        the sql client code (create_tables.py and etl.py) is used.  So that code may not include
        boto3 code or use the key & secret required to create a boto3 cleint. 

Note that this code uses the boto3 library

## Step 4 - Create Database Structure
 
1. Run ... python3 create_tables.py ... to  ...

    * creates the staging database tables and the data warehouse fact-dimension star schema optimized for queries on song play analysis

Note that this code uses the boto3 library.  It does not use the psychopg2 library.  This is simply because I wanted to learn about using the boto3 library for SQL commands.

## Step 5 - Populate the Database
 
1. Run ... python3 etl.py ... to  ...

    * extract from s3 to staging tables
transform and load from staging tables into the data warehouse fact-dimension star schema optimized for queries on song play analysis

Note - this uses the psychopg2 and boto3 librarys
Note - last time I ran this it took 45 minutes

## Step 6 - Data Analysis
 
1. Run ... python3 analysis.py ... runs sample queries in order for the results to provide insights leading to a deeper understand of the business

## Step 7 - Delete Infrastructure

1. Run ... python3 iac_delete.py ... to delete the AWS Serverless Redshift and its database

# Project Files

* iac_create.py - see above 
* create_tables.py - see above  
* etl.py - see above 
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
