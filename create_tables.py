""" Replace any existing tables with all new empty tables, 
    staging and fact-dimension tables """

import configparser
import boto3
from sql_queries import create_table_queries, drop_table_queries
import boto3_sql_util


def drop_tables(redshift_data_client, db_name, work_group_name):
    """ loop through list of DDL SQL to drop any existing 
    tables (that we are creating new versions of)"""

    for query in drop_table_queries:
        boto3_sql_util.execute_sql(redshift_data_client,
                                   query,
                                   db_name,
                                   work_group_name)


def create_new_tables(redshift_data_client, db_name, work_group_name):
    """ loop through list of DDL SQL to create new tables """
    for query in create_table_queries:
        boto3_sql_util.execute_sql(redshift_data_client,
                                   query,
                                   db_name,
                                   work_group_name)


def create_tables():
    """ Replace any existing tables with all new empty tables, 
    staging and fact-dimension tables """

    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    KEY = config.get('AWS', 'KEY')
    SECRET = config.get('AWS', 'SECRET')
    DWH_DB = config.get("DWH", "DWH_DB")
    DWH_REGION = config.get("DWH", "DWH_REGION")
    DWH_WORKGROUP_NAME = config.get("DWH", "DWH_WORKGROUP_NAME")

    redshift_data_client = boto3.client('redshift-data',
                                        region_name=DWH_REGION,
                                        aws_access_key_id=KEY,
                                        aws_secret_access_key=SECRET,
                                        )

    drop_tables(redshift_data_client, DWH_DB, DWH_WORKGROUP_NAME)
    create_new_tables(redshift_data_client, DWH_DB, DWH_WORKGROUP_NAME)


if __name__ == "__main__":
    create_tables()
