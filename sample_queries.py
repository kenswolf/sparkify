""" Data Analysis """

import configparser
import psycopg2
from prettytable import PrettyTable
from sql_queries import sample_queries, sample_query_titles
import utilities

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


def main():
    """ Run Sample Queries and Display Results """
    host = utilities.get_host(DWH_WORKGROUP_NAME, DWH_REGION, KEY, SECRET)

    conn = psycopg2.connect(
        f"host={host} dbname={DWH_DB} user={DWH_DB_USER} " +
        f"password={DWH_DB_PASSWORD} port={DWH_PORT}")
    cur = conn.cursor()

    for query, title in zip(sample_queries, sample_query_titles):
        cur.execute(query)
        table = PrettyTable()
        table.field_names = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
        for row in rows:
            table.add_row(row)
        print('-----------------------------------')
        print('')
        print(title)
        print('')
        print(query)
        print('')
        print(table)

    conn.commit()
    conn.close()


if __name__ == "__main__":
    main()
