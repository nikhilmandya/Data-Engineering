import configparser
import psycopg2
from sql_queries import copy_table_queries,insert_table_queries


def copy_table(cur,conn):
    print("copy data from the s3 to staging area")
    for query in copy_table_queries:
        print("executing",query)
        cur.execute(query)
        conn.commit()
    

def insert_table(cur,conn):
    print("inserting into table")
    for query in insert_table_queries:
        print("the data is copying",query)
        cur.execute(query)
        conn.commit()
        
def main():
    config=configparser.ConfigParser()
    config.read('dwh.cfg')
    
    
    conn=psycopg2.connect("postgresql://iammandya:spArk1f!@sparkifycluster.czirxbuwrjxb.us-east-2.redshift.amazonaws.com:5439/sparkifydb")
    cur=conn.cursor()
    print("connected to cluster")
    
    copy_table(cur,conn)
    insert_table(cur,conn)
    
    conn.close()
    
if __name__=="__main__":
    main()
