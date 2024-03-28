""" Utility Functions """

import configparser
import json
import time
import psycopg2


def get_duration_string(start_time):
    """ Create printable string of how long a process has taken so far"""

    duration = time.time() - start_time

    duration_minutes = int(duration // 60)
    duration_seconds = int(duration % 60)

    if duration_minutes == 0:
        duration_string = f"{duration_seconds} seconds"
    elif duration_minutes == 1:
        duration_string = f"1 minute and {duration_seconds} seconds"
    else:
        duration_string = f"{duration_minutes} minutes {duration_seconds} seconds"

    return duration_string


def get_db_connection():
    """ Replace any existing tables with all new empty tables, 
    staging and fact-dimension tables """

    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    HOST = config.get("CLUSTER", "HOST")
    DB_NAME = config.get("CLUSTER", "DB_NAME")
    DB_USER = config.get("CLUSTER", "DB_USER")
    DB_PASSWORD = config.get("CLUSTER", "DB_PASSWORD")
    DB_PORT = config.get("CLUSTER", "DB_PORT")

    conn = psycopg2.connect(
        f"host={HOST} dbname={DB_NAME} user={DB_USER} " +
        f"password={DB_PASSWORD} port={DB_PORT}")

    return conn
