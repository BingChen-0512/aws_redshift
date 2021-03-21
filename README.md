# Purpose of the Database 

Sparkify, a music streaming startup, has grown uswer base and want to move their processes
and data onto the cloud. The source data resides in S3. The Redshift database stores the data 
from S3 processed under ETL pipeline. The processed data in Redshift database makes analysis or
visualization convenient. Amazon Redshift is optimized for very fast execution of complex 
analytic queries against very large data sets. 

# Database Schema Design

The database schema was designed to be a star schema with a fact table (songplays) and four 
dimension tables (users, songs, artists, time).

#### Fact Table
1. **songplays**-records in log data asscociated with songs plays
- _songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent_
![alt text](https://github.com/BingChen-0512/data_modeling_postgres/blob/master/songplays.png?raw=true)

#### Dimension Tables
2. **users**-users in the app
- _user_id, first_name, last_name, gender, level_
![alt text](https://github.com/BingChen-0512/data_modeling_postgres/blob/master/songplays.png?raw=true)
3. **songs**-songs in music database
- _song_id, title, artist_id, year, duration_
![alt text](https://github.com/BingChen-0512/data_modeling_postgres/blob/master/songplays.png?raw=true)
4. **artists**-artists in music database
- _artist_id, name, location, latitude, longitude_
![alt text](https://github.com/BingChen-0512/data_modeling_postgres/blob/master/songplays.png?raw=true)
5. **time**-timestamps of records in **songplays** broken down into specific units
- _start_time, hour, day, week, month, year, weekday_
![alt text](https://github.com/BingChen-0512/data_modeling_postgres/blob/master/songplays.png?raw=true)

Only primary keys and foreign keys in the tables have NOT NULL constraint. 
The distrubution strategy is AUTO.

# ETL Pipline

The source data was first copied to staging tables. From staging tables, data was extracted to fit 
in the fact table and dimension tables. At the same time, the data was transformed from staging tables
to the target table. For example, by WHERE clause, some data we don't need was filtered out. By
transformation, the datatype of ts column in the raw data was changed from bigint to timestamp.
