"""   """

import configparser
import psycopg2 
import utilities
import pandas as pd

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


def query_one(cur: psycopg2.extensions.cursor, conn: psycopg2.extensions.connection):
    """  abc """ 

    query = "SELECT user_id, first_name, last_name, gender, level FROM users"
    df = pd.read_sql_query(query, conn)
    print(df)

def query_two(cur: psycopg2.extensions.cursor, conn: psycopg2.extensions.connection): 
    for query in []:
        cur.execute(query)
        conn.commit()

def query_three(cur: psycopg2.extensions.cursor, conn: psycopg2.extensions.connection): 
    for query in []:
        cur.execute(query)
        conn.commit()


def main(): 
    host = utilities.get_host(DWH_WORKGROUP_NAME, DWH_REGION, KEY, SECRET)

    conn = psycopg2.connect(
        f"host={host} dbname={DWH_DB} user={DWH_DB_USER}" +
        f"password={DWH_DB_PASSWORD} port={DWH_PORT}")
    cur = conn.cursor()

    query_one(cur, conn) 

    conn.commit()
    conn.close()


if __name__ == "__main__":
    main()
