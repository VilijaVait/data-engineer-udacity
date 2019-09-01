from time import time
import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        t0 = time()
        cur.execute(query)
        conn.commit()
        print('Executed query:{}Run time {} secs'.format(query, round(time()-t0,4)))

def insert_tables(cur, conn):
    for query in insert_table_queries:
        t0 = time()
        cur.execute(query)
        conn.commit()
        print('Executed query:{}Run time {} secs'.format(query, round(time()-t0,4)))
        

def main():
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