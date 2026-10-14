-- ─────────────────────────────────────────────────────────────────────────────
-- spotify_pipeline/db/athena_schema.sql
--
-- External table over the S3 data lake written by ingest.py.
-- Partitioned by date (run date) and genre.
-- Replace YOUR_BUCKET_NAME and YOUR_DATABASE with your actual values.
-- ─────────────────────────────────────────────────────────────────────────────

CREATE DATABASE IF NOT EXISTS spotify_analytics;

CREATE EXTERNAL TABLE IF NOT EXISTS spotify_analytics.raw_tracks (
    track_id          STRING,
    artists           STRING,
    album_name        STRING,
    track_name        STRING,
    popularity        INT,
    duration_ms       BIGINT,
    explicit          BOOLEAN,
    danceability      DOUBLE,
    energy            DOUBLE,
    key               INT,
    loudness          DOUBLE,
    mode              INT,
    speechiness       DOUBLE,
    acousticness      DOUBLE,
    instrumentalness  DOUBLE,
    liveness          DOUBLE,
    valence           DOUBLE,
    tempo             DOUBLE,
    time_signature    INT,
    ingested_at       STRING,
    run_date          STRING
)
PARTITIONED BY (
    date   STRING,   -- run date: YYYY-MM-DD
    genre  STRING    -- lowercase, underscored genre name
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
    'ignore.malformed.json' = 'TRUE'
)
STORED AS TEXTFILE
LOCATION 's3://YOUR_BUCKET_NAME/raw/'
TBLPROPERTIES ('has_encrypted_data' = 'false');

-- Run after new data lands to register new partitions:
MSCK REPAIR TABLE spotify_analytics.raw_tracks;
