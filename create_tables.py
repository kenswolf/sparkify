""" Replace any existing tables with all new empty tables, 
    staging and fact-dimension tables """

import psycopg2
import utilities
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur: psycopg2.extensions.cursor, conn: psycopg2.extensions.connection):
    """ loop through list of DDL SQL to drop any existing 
    tables (that we are creating new versions of)"""

    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_new_tables(cur: psycopg2.extensions.cursor, conn: psycopg2.extensions.connection):
    """ loop through list of DDL SQL to create new tables """

    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """ Replace any existing tables with all new empty tables, 
    staging and fact-dimension tables """

    conn = utilities.get_db_connection()
    cur = conn.cursor()
    drop_tables(cur, conn)
    create_new_tables(cur, conn)
    conn.close()


if __name__ == "__main__":
    main()
