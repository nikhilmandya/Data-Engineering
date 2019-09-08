import configparser

config=configparser.ConfigParser()
config.read('dwh.cfg')

#Drop tables
staging_events_table_drop="drop table if exists staging_events;"
staging_songs_table_drop="drop table if exists staging_songs;"
songplay_table_drop="drop table if exists songplays;"
user_table_drop="drop table if exists users;"
song_table_drop="drop table if exists songs;"
artist_table_drop="drop table if exists artists;"
time_table_drop="drop table if exists time;"


# Create table
staging_events_table_create=("""create table staging_events(
        event_id bigint identity(0,1)       not null,
        artist varchar                          null,
        auth    varchar                         null,
        firstName varchar                       null,
        gender varchar                          null,
        itemInSession  varchar                  null,
        lastName varchar                        null,
        level       varchar                     null,
        location    varchar                     null,
        method      varchar                     null,
        page        varchar                     null,
        registration varchar                    null,
        sessionId   integer                     not null sortkey distkey,
        song        varchar                     null,
        status      varchar                     null,
        ts          bigint                      null,
        userAgent   varchar                     null,
        userId      integer                     null);""")
        
        
staging_songs_table_create=("""create table staging_songs(
    num_songs          integer null,
    artist_id          varchar not null sortkey distkey,
    artist_latitude     varchar null,
    artist_longitude    varchar null,
    artist_location     varchar(500) null,
    artist_name         varchar(500) null,
    song_id             varchar     not null,
    title               varchar(500) null,
    duration            decimal(9) null,
    yeat                integer    null);""")
    

#Analytics tables
songpay_table_create=("""create table if not exists songplays(
    songplay_id         integer identity(1,1) not null sortkey,
    start_time          timestamp       not null,
    user_id             varchar(50)     not null distkey,
    level               varchar(10)     null,
    song_id             varchar(40)     not null,
    artist_id           varchar(50)     not null,
    session_id          integer         not null,
    location            varchar(100)    null,
    user_agent          varchar(255)    null);""")
    
    
user_table_create=("""create table if not exists users(
    user_id         integer     not null sortkey,
    first_name      varchar(50) null,
    last_name       varchar(50) null,
    gender          varchar(50) null,
    level           varchar(50) null)diststyle all;"""
)

song_table_create=("""create table if not exists songs(
    songs_id    varchar(50)     not null sortkey,
    title       varchar(500)    not null,
    artist_id   varchar(50)     not null,
    year        integer         not null,
    duration    decimal(9)      not null);"""
)

        
        
artist_table_create=("""create table if not exists artists(
    artist_id   varchar(50)     not null sortkey,
    name        varchar(500)    null,
    location    varchar(500)    null,
    latitude    decimal(9)      null,
    longitude   decimal(9)      null)diststyle all;"""
)

time_table_create=("""create table if not exists time(
    start_time  timestamp   not null sortkey,
    hour        smallint    null,
    day         smallint    null,
    week        smallint    null,
    month       smallint    null,
    year        smallint    null,
    weekday     smallint    null)diststyle all;"""
    )


# STAGING TABLES

S3_LOG_DATA = config.get('S3', 'LOG_DATA')
S3_LOG_JSONPATH = config.get('S3', 'LOG_JSONPATH')
S3_SONG_DATA = config.get('S3', 'SONG_DATA')
DWH_IAM_ROLE_ARN = config.get("IAM_ROLE", "ARN")

staging_events_copy = ("""

    copy staging_events
    from '{}'
    iam_role '{}'
    compupdate off statupdate off
    format as json '{}'
    timeformat as 'epochmillisecs'

""").format(S3_LOG_DATA, DWH_IAM_ROLE_ARN, S3_LOG_JSONPATH)


staging_songs_copy = ("""
    copy staging_songs from {data_bucket}
    credentials 'aws_iam_role={role_arn}'
    region 'us-west-2' format as JSON 'auto';
""").format(data_bucket=config['S3']['SONG_DATA'], role_arn=config['IAM_ROLE']['ARN'])

#insert into tables
user_table_insert=("""
insert into users(user_id,first_name,last_name,gender,level)
select distinct(userId) as user_id,
firstName as first_name,
lastName as last_name,
gender,
level
from staging_events
where user_id is not null and page=='NextSong';"""
)


song_table_insert=("""insert into songs (songs_id,title,artist_id,year,duration)
select distinct(song_id)as song_id,
title,
artist_id,
year,
duration
from staging_songs
where song_id is not null;""")


artist_table_insert=("""insert into artists(artist_id,name,location,latitude,longitude)
    select distinct(artist_id) as artist_id,
    artist_name as name,
    artist_location as location,
    artist_latitude as latitude,
    artist_longitude as longitude
    from staging_songs where artist_id is not null;"""
)
time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT  DISTINCT(start_time)                AS start_time,
            EXTRACT(hour FROM start_time)       AS hour,
            EXTRACT(day FROM start_time)        AS day,
            EXTRACT(week FROM start_time)       AS week,
            EXTRACT(month FROM start_time)      AS month,
            EXTRACT(year FROM start_time)       AS year,
            EXTRACT(dayofweek FROM start_time)  as weekday
    FROM songplays;
""")


songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT  DISTINCT(e.ts)  AS start_time,
            e.userId        AS user_id,
            e.level         AS level,
            s.song_id       AS song_id,
            s.artist_id     AS artist_id,
            e.sessionId     AS session_id,
            e.location      AS location,
            e.userAgent     AS user_agent
    FROM staging_events e
    JOIN staging_songs  s   ON (e.song = s.title AND e.artist = s.artist_name)
    AND e.page  ==  'NextSong'
""")

# GET NUMBER OF ROWS IN EACH TABLE
get_number_staging_events = ("""
    SELECT COUNT(*) FROM staging_events
""")

get_number_staging_songs = ("""
    SELECT COUNT(*) FROM staging_songs
""")

get_number_songplays = ("""
    SELECT COUNT(*) FROM songplays
""")

get_number_users = ("""
    SELECT COUNT(*) FROM users
""")

get_number_songs = ("""
    SELECT COUNT(*) FROM songs
""")

get_number_artists = ("""
    SELECT COUNT(*) FROM artists
""")

get_number_time = ("""
    SELECT COUNT(*) FROM time
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songpay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
select_number_rows_queries= [get_number_staging_events, get_number_staging_songs, get_number_songplays, get_number_users, get_number_songs, get_number_artists, get_number_time]



