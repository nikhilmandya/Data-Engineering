# Create Fact table

###########################
#    Songs play  Fact table       #
##########################
songplay_create=("""
    CREATE TABLE IF NOT EXISTS songplays
    (songplay_id int PRIMARY KEY,
    start_time date REFERENCES time(start_time),
    user_id int NOT NULL REFERENCES users(user_id),
    level text,
    song_id text REFERENCES songs(song_id),
    artist_id text REFERENCES artists(artist_id),
    session_id int,
    location text,
    user_agent text)
""")



###########################
#    user dimensions table       #
##########################

user_table_create="create table if not exists users(user_id int primary key,first_name text NOT NULL, last_name text NOT NULL, gender text, level text)"

#
#user_table_create = ("""
#    CREATE TABLE IF NOT EXISTS users
#    (user_id int PRIMARY KEY,
#    first_name text NOT NULL,
#    last_name text NOT NULL,
#    gender text,
#    level text)
#""")

###########################
#    song_table dimension       #
##########################

song_table_create="create table if not exists songs(song_id text primary key, title text not null, artist_id text not null references artists(artist_id),year int, duration float not null)"


###########################
#    Artist dimension  table       #
##########################

artist_table_create="create table if not exists artists(artist_id text primary key, name text not null,location text, lattitude float, longitude float)"


###########################
#  Time_table dimension table    #
##########################

time_table_create=("""create table if not exists time(start_time date primary key, hour int, day int, week int, month int, year int, weekday text)""")



################################################

# code to insert


###############################################



songplay_insert="insert into songplays(songplay_id, start_time,user_id,level,song_id,artist_id,session_id, location,user_agent)values(%s,%s,%s,%s,%s,%s,%s,%s,%s) on conflict (songplay_id) do nothing"

user_insert="insert into users(user_id,first_name,last_name,gender,level)values(%s,%s,%s,%s,%s) on conflict(user_id) do nothing"

songs_insert="insert into songs(song_id,title,artist_id,year,duration)values(%s,%s,%s,%s,%s)ON CONFLICT (song_id) DO NOTHING;"


artists_insert="insert into artists(artist_id,name,location,lattitude,longitude)values(%s,%s,%s,%s,%s) on conflict(artist_id) do nothing"

time_insert="insert into time(start_time,hour,day,week,month,year,weekday)values(%s,%s,%s,%s,%s,%s,%s)on conflict(start_time) do nothing"


##################
#              drop every table
##################



songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"




# FIND SONGS

song_select = ("""
    SELECT song_id, artists.artist_id
    FROM songs JOIN artists ON songs.artist_id = artists.artist_id
    WHERE songs.title = %s
    AND artists.name = %s
    AND songs.duration = %s
""")




#############
# put everything in a list

create_table_queries=[user_table_create, artist_table_create,song_table_create, time_table_create,songplay_create]

list_insert=[songplay_insert,user_insert,songs_insert,artists_insert,time_insert]

drop_table_queries= [user_table_drop, artist_table_drop, song_table_drop, time_table_drop, songplay_table_drop]




