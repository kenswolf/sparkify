""" Generic SQL Functionality Utility Functions """


import time
from datetime import datetime
import boto3
import pandas as pd


def get_query_results_as_a_dataframe(result) -> pd.DataFrame:
    """ load the results of a sql query into a pandas DataFrame """

    columns = [col['name'] for col in result['ColumnMetadata']]

    # Initialize an empty list to store the transformed results
    transformed_results = []

    # Iterate through each row in the result
    for row in result['Records']:
        # Create a dictionary for the current row
        row_dict = {}
        for col, val in zip(columns, row):
            # Each value in the row is a dictionary with a single key-value pair
            # The key indicates the data type, and the value is the data itself
            # Here, we extract the value and add it to the row dictionary
            data_val = list(val.values())[0] if val else None
            row_dict[col] = data_val

        # Add the transformed row to the results list
        transformed_results.append(row_dict)

    # Convert the query results into a pandas DataFrame
    df = pd.DataFrame(transformed_results)

    return df


def execute_sql(redshift_data: boto3.session.Session.resource,
                _query: str,
                db: str,
                work_group_name: str) -> None:
    """ Execute Generic SQL on Database """

    start = datetime.now()

    response = redshift_data.execute_statement(
        Database=db,  # 'dev',  #'dwh',  Use your default database to connect
        Sql=_query,
        WorkgroupName=work_group_name  # Specify your serverless workgroup name
    )

    print('---------')
    print(_query)
    print(response)
    query_id = response['Id']
    print(redshift_data.describe_statement(Id=query_id))
    print('---------')

    while True:
        status_response = redshift_data.describe_statement(Id=query_id)
        status = status_response['Status']
        print(status, (datetime.now() - start))
        if status == 'FINISHED':
            break
        elif status in ['FAILED', 'ABORTED']:
            # raise Exception(f"SQL query failed or was aborted: {status_response}")
            print(f"SQL query failed or was aborted: {status_response}")
            break
        time.sleep(1)  # Wait for a bit before checking again

    print('---------')

    if status_response['Status'] == 'FINISHED':
        if status_response.get('HasResultSet', False):
            result = redshift_data.get_statement_result(Id=query_id)
            df = get_query_results_as_a_dataframe(result)
            print(df)

        else:
            print("SQL executed successfully, but no results were returned.")


def debug_failures_from_load_of_s3_data_into_staging_tables(redshift_data,
                                                            db_name: str,
                                                            work_group_name: str) -> None:
    """ Query system table that contains messages from load failures caused by copy command """

    sql = """
        SELECT query_id,
            table_id,
            start_time,
            trim(file_name) AS file_name, 
            trim(column_name) AS column_name, 
            trim(column_type) AS column_type, 
            trim(error_message) AS error_message 
        FROM sys_load_error_detail 
        """

    execute_sql(redshift_data, sql, db_name, work_group_name)
