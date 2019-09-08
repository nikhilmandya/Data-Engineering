import configparser
import psycopg2
from sql_queries import create_table_queries,drop_table_queries


def drop_tables(conn,cur):
    print("deleting the table we have")
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()
    

def create_tables(conn,cur):
    print("creating the new tables")
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
        
def main():
    config=configparser.ConfigParser()
    
    config.read('dwh.cfg')
    conn=psycopg2.connect("postgresql://iammandya:spArk1f!@sparkifycluster.czirxbuwrjxb.us-east-2.redshift.amazonaws.com:5439/sparkifydb")
    cur=conn.cursor()
    print("connected to cluster")
    
    drop_tables(conn,cur)
    create_tables(conn,cur)
    
    conn.close()
    
if __name__=="__main__":
    main()
