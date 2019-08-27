import os
import pandas as pd
import psycopg2
import numpy as np
from sql_queries import *
import glob

#Insert data into the database
def insert_from_frame(cur,df,insert_query):
    ##
    # Go trough all the lines in the df and add to db
    for i,row in df.iterrows():
        cur.execute(insert_query,list(row))



#Process song_files i.e go through all the json files
def song_files(cur,filepath):
    df=pd.read_json(filepath,lines=True)
    #artist_table_create="create table if not exists artists
    # (artist_id text primary key, name text not null,location text, lattitude float, longitude float)"

    artist_data=df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']]
    # So the artist_Data is a df
    artist_data=artist_data.drop_duplicates()
    artist_data=artist_data.replace(np.NaN,np.nan)

    insert_from_frame(cur,artist_data,artists_insert)


    #for song db(song_id,title,artist_id,year,duration)

    ### insert into song db
    song_data=df[['song_id','title','artist_id','year','duration']]
    song_data=song_data.drop_duplicates()
    song_data=song_data.replace(np.NaN,np.nan)
    insert_from_frame(cur,song_data,songs_insert)

##Log files

def log_files(cur,filepath):
    df=pd.read_json(filepath,lines=True)
    print(df.shape)
    df=df[df.page=='NextSong']
    tf=pd.DataFrame({'start_time':pd.to_datetime(df['ts'],unit='ms')})

    # Creating new columns
    tf['hour'] = tf['start_time'].dt.hour
    tf['day'] = tf['start_time'].dt.day
    tf['week'] = tf['start_time'].dt.week
    tf['month'] = tf['start_time'].dt.month
    tf['year'] = tf['start_time'].dt.year
    tf['weekday'] = tf['start_time'].dt.weekday


    tf=tf.drop_duplicates()
    insert_from_frame(cur,tf,time_insert)

    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]
    user_df=user_df.drop_duplicates()
    user_df=user_df.replace(np.NaN,np.nan)
    # sometimes the colums arrangements may change to get in an order as we required
    user_df.columns = ['user_id', 'first_name', 'last_name', 'gender', 'level']

    insert_from_frame(cur,user_df,user_insert)

    for i, row in df.iterrows():
        cur.execute(song_select, (row.song, row.artist, row.length))

        result = cur.fetchone()
        #     print(result)
        if result:
            songid, artistid = result
        else:
            songid, artistid = None, None
        # song_data = (i, row.time_data, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        # cur.execute(songplay_insert, song_data)
        # conn.commit()



def process_data(cur,conn,filepath,func):
    all_files=[]
    for root,dirs,files in os.walk(filepath):
        files=glob.glob(os.path.join(root,'*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))

    print("total number of files are ",len( all_files))
    # the approach here taken is go to file and upload to db the other method is to aggregate all the date to the single file and then update the db
    final_file=pd.read_json(all_files[0],lines=True)
    # print(final_file.shape)
    # for file in all_files[1:]:
    #     df2=pd.read_json(file,lines=True)
    #     final_file=pd.concat([final_file,df2],ignore_index=True)
        # print(final_file.shape)
    for file in all_files:
        func(cur,file)
        conn.commit()


def main():
    conn=psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=postgres password=macbook")
    cur=conn.cursor()
    process_data(cur,conn,'data/song_data',song_files)
    process_data(cur,conn,'data/log_data',log_files)

    conn.close()

if __name__=="__main__":
    main()


    
