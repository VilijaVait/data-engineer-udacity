from time import time
import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Description: This function can be used to load data into staging tables,
    using queries specified within copy_table_queries list, and to time each
    query execution.  One table is loaded per query.
    Prints out query and loading time.

    Arguments:
        cur: the cursor object,
        conn: the connection object 
        
    Returns:
        None
    """

    for query in copy_table_queries:
        t0 = time()
        cur.execute(query)
        conn.commit()
        print('Executed query:{}Run time {} secs'.format(query, round(time()-t0,4)))

def insert_tables(cur, conn):
    """
    Description: This function can be used to insert data into final schema tables,
    using queries specified within insert_table_queries list, and to time each
    query execution.  One table is populated per query.
    Prints out query and loading time.

    Arguments:
        cur: the cursor object,
        conn: the connection object 
        
    Returns:
        None
    """

    for query in insert_table_queries:
        t0 = time()
        cur.execute(query)
        conn.commit()
        print('Executed query:{}Run time {} secs'.format(query, round(time()-t0,4)))
        

def main():
    """
    Description: This program can be used to load data to staging and final schema tables within 
    in a given redshift cluster. Queries, which detail the loading process, are imported from 
    sql_queries.py.
    The redshift cluster connection parameters are defined in 'dwh.cfg' file.

    Arguments:
        None
    Returns:
        None
    """

    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    print('LOADING STAGING TABLES>>')
    load_staging_tables(cur, conn)
    
    print('LOADING SCHEMA TABLES>>')
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()