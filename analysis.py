""" Data Analysis """

import psycopg2
from sql_queries import sample_queries, sample_query_titles
import utilities


def run_analysis_queries(cur: psycopg2.extensions.cursor):
    """ Run Sample Queries and Display Results """

    for query, title in zip(sample_queries, sample_query_titles):

        cur.execute(query)

        headers_list = [desc[0] for desc in cur.description]
        header_string = '\t'.join(headers_list)

        headers_sizes_list = [len(header) for header in headers_list]
        headers_underscore_list = [
            '-' * header_size for header_size in headers_sizes_list]
        header_underscore_string = '\t'.join(headers_underscore_list)

        print('-----------------------------------')
        print('')
        print(title, '(truncating display at 35 rows of query results)')
        print('')
        print(query)
        print('')
        print(header_string)
        print(header_underscore_string)

        count = 0
        while True:
            row = cur.fetchone()
            if row is None or count == 35:
                break

            padded_values_list = [str(val) + (' ' * (header_size - len(str(val))))
                                  for val, header_size in zip(row, headers_sizes_list)]

            print('\t'.join(padded_values_list))

            count += 1

        print('')


def main():
    """ Run Analysis queries as a stand alone program """

    conn = utilities.get_db_connection()
    cur = conn.cursor()
    run_analysis_queries(cur)
    conn.close()


if __name__ == "__main__":
    main()
