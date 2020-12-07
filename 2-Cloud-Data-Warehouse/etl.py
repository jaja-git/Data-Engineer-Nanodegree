import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
from tqdm import tqdm 

def load_staging_tables(cur, conn):
    """
    This function loads data to the staging tables
    """
    for i in tqdm(range(0,100)):
        for query in copy_table_queries:
            cur.execute(query)
            conn.commit()


def insert_tables(cur, conn):
    """
    This function copies data from the staging tables to the production table
    """
    for i in tqdm(range(0,100)):
        for query in insert_table_queries:
            cur.execute(query)
            conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['DWH'].values()))
    cur = conn.cursor()
    
    #load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()