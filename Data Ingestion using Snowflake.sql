-- Creating a Stage for extracting Data
CREATE OR REPLACE STAGE SPF_STAGE
URL = 's3://bucketName'
CREDENTIALS = (AWS_KEY_ID = '###' AWS_SECRET_KEY = '###');

-- Creating a file format
CREATE OR REPLACE FILE FORMAT CSVFILEFORMAT
TYPE = CSV
SKIP_HEADER = 1
FIELD_DELIMITER = ',';

-- Creating different tables to store album, artists and songs data
CREATE OR REPLACE TABLE ALBUM(
    album_ID string,
    album_name STRING,
    album_href STRING,
    album_popularity number,
    album_total_tracks INT
);

CREATE OR REPLACE TABLE SONGS(
    song_ID string,
    song_name STRING,
    song_url STRING,
    song_release_date DATE,
    song_duration INT 
);

CREATE OR REPLACE TABLE ARTISTS(
    artist_ID string,
    artist_name	STRING,
    artist_href STRING
);

--  creating separate snowpipes for data ingestion
CREATE OR REPLACE PIPE ALBUM_PIPE
    AUTO_INGEST = TRUE
    AS 
    COPY INTO ALBUM(album_ID, album_name, album_href, album_popularity, album_total_tracks)
    FROM @SPF_STAGE/albumFolderNameOnS3
    file_format = csvfileformat
    on_error = continue;


CREATE OR REPLACE PIPE SONGS_PIPE
    AUTO_INGEST = TRUE
    AS 
    COPY INTO SONGS(song_ID, song_name, song_url, song_release_date,song_duration)
    FROM @SPF_STAGE/songsFolderNameOnS3
    file_format = csvfileformat
    on_error = continue;


CREATE OR REPLACE PIPE ARTISTS_PIPE
    AUTO_INGEST = TRUE
    AS 
    COPY INTO ARTISTS(artist_ID, artist_name, artist_href)
    FROM @SPF_STAGE/artistsFolderNameOnS3
    file_format = csvfileformat
    on_error = continue;

-- Once snowpipes are created now desc pipes and copy notification arn and paste it to the source S3 bucket























