import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

IAM_ROLE = config['IAM_ROLE']['ARN']
LOG_DATA = config['S3']['LOG_DATA']
SONG_DATA = config['S3']['SONG_DATA']
LOG_JSONPATH = config['S3']['LOG_JSONPATH']
REGION = config['REGION']['REGION_NAME']
# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES
# consider of loading speed, it's recommended not to add anything to staging
# tables, even no constraints
staging_events_table_create= ("""
    CREATE TABLE staging_events
    (
      artist         varchar,
      auth           varchar,
      firstName      varchar,
      gender         char(1),
      itemInSession  integer,
      lastName       varchar,
      length         float,
      level          char(4),
      location       varchar,
      method         varchar,
      page           varchar,
      registration   bigint,
      sessionId      integer,
      song           varchar,
      status         integer,
      ts             bigint,
      userAgent      varchar,
      userId         integer
    )
""")

staging_songs_table_create = ("""
    CREATE TABLE staging_songs
    (
      artist_id          varchar,
      artist_latitude    float,
      artist_longitude   float,
      artist_location    varchar,
      artist_name        varchar,
      duration           float,
      num_songs          integer,
      song_id            varchar,
      title              varchar,
      year               integer
    )
""")

songplay_table_create = ("""
    CREATE TABLE songplays
    (
      songplay_id INT IDENTITY(0,1) PRIMARY KEY,
      start_time  timestamp         not null,
      user_id     int               not null,
      level       varchar           not null,
      song_id     VARCHAR,
      artist_id   VARCHAR,
      session_id  int               not null,
      location    varchar           not null,
      user_agent  varchar           not null
     )
""")
# for dim tables, distkey based on columns to be joined later between TABLES
# and on the size of the table. If the size is small, put the table on all nodes
user_table_create = ("""
    CREATE TABLE users
    (
      user_id   int     PRIMARY KEY,
      first_name varchar NOT NULL,
      last_name  varchar NOT NULL,
      gender    varchar NOT NULL,
      level     varchar NOT NULL
    )
""")

song_table_create = ("""
    CREATE TABLE songs
    (
      song_id   VARCHAR PRIMARY KEY,
      title     varchar NOT NULL,
      artist_id VARCHAR NOT NULL,
      year      int     NOT NULL,
      duration  numeric NOT NULL
    )
""")

artist_table_create = ("""
    CREATE TABLE artists
    (
      artist_id VARCHAR PRIMARY KEY,
      name      varchar NOT NULL,
      location  varchar NOT NULL,
      latitude  numeric,
      longitude numeric
    )
""")

time_table_create = ("""
    CREATE TABLE time(
    start_time timestamp PRIMARY KEY,
    hour       int       NOT NULL,
    day        int       NOT NULL,
    week       int       NOT NULL,
    month      int       NOT NULL,
    year       int       NOT NULL,
    weekday    varchar   NOT NULL
    )
""")

# STAGING TABLES
# define json can keep the format of column names. Column names will be converted to lower case by default.
staging_events_copy = ("""
    copy staging_events
    from {}
    iam_role {}
    region {}
    COMPUPDATE OFF
    json {};
""").format(LOG_DATA, IAM_ROLE, REGION, LOG_JSONPATH)

staging_songs_copy = ("""
    copy staging_songs
    from {}
    iam_role {}
    json 'auto'
    COMPUPDATE OFF
    region {};
""").format(SONG_DATA, IAM_ROLE, REGION)
# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT DISTINCT timestamp 'epoch' + se.ts/1000 * interval '1 second' as start_time,
           e.userId,
           e.level,
           s.song_id,
           s.artist_id,
           e.sessionId,
           e.location,
           e.userAgent
    FROM staging_events e
    JOIN staging_songs s
    on (e.song = s.title and e.artist = s.artist_name)
    where e.page = 'NextSong'
""")

user_table_insert = ("""
    INSERT INTO users (user_id, firstName, lastName, gender, level)
    SELECT DISTINCT userId,
                    firstName,
                    lastName,
                    gender,
                    level
    FROM staging_events
    WHERE page = 'NextSong'
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT DISTINCT song_id,
                    title,
                    artist_id,
                    year,
                    duration
    FROM staging_songs
    WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT artist_id,
                    artist_name,
                    artist_location,
                    artist_latitude,
                    artist_longitude
    FROM staging_songs
    WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT start_time,
                    EXTRACT(hour FROM start_time),
                    EXTRACT(day FROM start_time),
                    EXTRACT(week FROM start_time),
                    EXTRACT(month FROM start_time),
                    EXTRACT(year FROM start_time),
                    EXTRACT(dayofweek FROM start_time)
                    FROM songplays
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
