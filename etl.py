""" This ETL code reads from S3 and populates staging 
and fact-dimension tables in a database tables associated 
with a Serverless Redshift """

import configparser
import psycopg2
import utilities
import analysis
from sql_queries import copy_table_queries, insert_table_queries

LOG_DATA = 's3://udacity-dend/log-data'
LOG_JSONPATH = 's3://udacity-dend/log_json_path.json'
SONG_DATA = 's3://udacity-dend/song-data/A/A'
S3_REGION = 'us-west-2'

config = configparser.ConfigParser()
config.read('dwh.cfg')
ROLE_ARN = config.get("IAM_ROLE", "ARN")


def load_staging_tables(cur: psycopg2.extensions.cursor, conn: psycopg2.extensions.connection):
    """ Extracts data from S3 into staging database tables """

    for query in copy_table_queries:

        if 'staging_songs' in query:
            query = query.format(SONG_DATA, ROLE_ARN, S3_REGION)
        elif 'staging_events' in query:
            query = query.format(LOG_DATA, ROLE_ARN, LOG_JSONPATH, S3_REGION)

        print('\n', query, '\n')

        cur.execute(query)
        conn.commit()


def insert_tables(cur: psycopg2.extensions.cursor, conn: psycopg2.extensions.connection):
    """ Transforms and loads data in staging tables into fact-dimension tables"""
    for query in insert_table_queries:

        print('\n', query, '\n')

        cur.execute(query)
        conn.commit()


def main():
    """ This method controls the ETL processes """

    conn = utilities.get_db_connection()
    cur = conn.cursor()

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    print('\n\nETL completed. Starting analysis queries.\n\n')
    analysis.run_analysis_queries(cur)

    conn.commit()
    conn.close()


if __name__ == "__main__":
    main()
