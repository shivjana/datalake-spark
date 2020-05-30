OBJECTIVE

Sparkify is a music streaming app and as part of user-analysis, they analyze the data which are mainly composed of log files on songs and user activity. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app. The primary objective of their analysis is to determine what songs users are mostly interested in and as part of achieving the objective we build an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables.

ETL PIPELINING

The JSON log files are located in Amazon 3 as 2 directories, one containing user activity related files and other containing song related files. Spark session is created and the object containing the session will be used to load the json files from S3 into a spark dataframe. A customized schema is used to infer the data types for columns from read json file. This data is checked for any corrupt record, invalid datatype, invalid data, duplicates and existence of empty strings for some of the columns as seen in analysis.ipynb file. For the time table, since we need to break down the timestamp into different time units, the timestamp read from the log file will be converted into a datetime object and then broken down using the datetime attribute. The songs and artist table are populated by reading the log files from the directory containing song and artist data. The users and time table are populated by reading the timestamped log files in another directory. In order to fetch the song_id and artist_id from the song and artist table for populating in songplay table, song and artist table are joined together and matched against the song title and artist name from the log files. The other attributes in songplay are inserted by reading the timestamped files.

TRANSFORM AND WRITE-BACK

The tables are created such that analysts can create a optimized query on both the songs and the users. Star schema will be used for the schema design with 5 tables namely songplay, which will be the fact table containing information about the user usage and hence a fact table, users, which will have information about the users using the app, songs, which will have information about the songs played in the app, artists, which will have information about the artists playing the songs played in the app, and time, which will have information about the timestamps of the records in songplay broken into time,day,month,week,weekday and year. The songplay table will be associated through the primary keys of the dimension tables, namely user_id from the users table, song_id from the song table, artist_id from the artists table, and start_time from time table.Since the primary objective of the analysis is to understand what the users are listening to, the songplay table will be helpful in gving us facts related to usage by combining the information referred from other dimension tables

The tables are then witten back to S3 as separate directories in S3 as parquet files.

CODE EXECUTION

In order to execute the python files:

python etl.py
