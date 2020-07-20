import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
from sqlalchemy import create_engine


def load_staging_tables(conn):
    for query in copy_table_queries:
        conn.execute(query)
        #conn.commit()


def insert_tables(conn):
    for query in insert_table_queries:
        conn.execute(query)
        #conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    host = config['CLUSTER']['HOST']
    dbname = config['CLUSTER']['DB_NAME']
    user=config['CLUSTER']['DB_USER']
    password = config['CLUSTER']['DB_PASSWORD']
    port = config['CLUSTER']['DB_PORT']
    
    
    conn_string="postgresql://{}:{}@{}:{}/{}".format(user, password, host, port, dbname)
    conn = create_engine(conn_string)
    
    load_staging_tables(conn)
    insert_tables(conn)

    


if __name__ == "__main__":
    main()