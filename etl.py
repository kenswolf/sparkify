import configparser
import requests
import psycopg2
import boto3
from sql_queries import copy_table_queries, insert_table_queries
import ingress_rule

config = configparser.ConfigParser()
config.read('dwh.cfg')

KEY = config.get('AWS', 'KEY')
SECRET = config.get('AWS', 'SECRET')

DWH_PORT = config.get("DWH", "DWH_PORT")
DWH_REGION = config.get("DWH", "DWH_REGION")
DWH_IAM_ROLE_NAME = config.get("DWH", "DWH_IAM_ROLE_NAME")
DWH_WORKGROUP_NAME = config.get("DWH", "DWH_WORKGROUP_NAME")

DWH_DB = config.get("DWH", "DWH_DB")
DWH_DB_USER = config.get("DWH", "DWH_DB_USER")
DWH_DB_PASSWORD = config.get("DWH", "DWH_DB_PASSWORD")


def get_host():

    redshift_serverless_client = boto3.client('redshift-serverless',
                                              region_name=DWH_REGION,
                                              aws_access_key_id=KEY,
                                              aws_secret_access_key=SECRET,
                                              )

    workgroup = redshift_serverless_client.get_workgroup(
        workgroupName=DWH_WORKGROUP_NAME)
    endpoint = workgroup['workgroup']['endpoint']
    return endpoint['address']


def get_role_arn():

    iam_client = boto3.client('iam',
                              region_name=DWH_REGION,
                              aws_access_key_id=KEY,
                              aws_secret_access_key=SECRET
                              )

    return iam_client.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']


def load_staging_tables(cur, conn):

    role_arn = get_role_arn()

    for query in copy_table_queries:
        query = query.format(role_arn)
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():

    host = get_host()

    conn = psycopg2.connect(
        f"host={host} dbname={DWH_DB} user={DWH_DB_USER} password={DWH_DB_PASSWORD} port={DWH_PORT}")
    cur = conn.cursor()

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.commit()
    conn.close()


if __name__ == "__main__":
    main()
