-- dbt model: artist_popularity
-- Top artists by average track popularity and total track count.
-- Handles multi-artist tracks (semicolon-separated) by attributing
-- popularity to the first listed artist.

WITH deduped AS (
    SELECT
        track_id,
        -- Take first artist when multiple are listed (e.g. "Artist A;Artist B")
        TRIM(SPLIT_PART(artists, ';', 1))  AS primary_artist,
        genre,
        popularity,
        danceability,
        energy,
        valence,
        ROW_NUMBER() OVER (
            PARTITION BY track_id
            ORDER BY date DESC
        ) AS rn
    FROM {{ source('spotify_analytics', 'raw_tracks') }}
),

latest AS (SELECT * FROM deduped WHERE rn = 1),

aggregated AS (
    SELECT
        primary_artist,
        COUNT(DISTINCT track_id)          AS total_tracks,
        ROUND(AVG(popularity), 1)         AS avg_popularity,
        MAX(popularity)                   AS peak_popularity,
        ROUND(AVG(danceability), 3)       AS avg_danceability,
        ROUND(AVG(energy), 3)             AS avg_energy,
        ROUND(AVG(valence), 3)            AS avg_valence,
        -- Most common genre for this artist
        MAX_BY(genre, COUNT(*))           AS primary_genre
    FROM latest
    GROUP BY primary_artist
    HAVING COUNT(DISTINCT track_id) >= 3  -- filter one-hit wonders
)

SELECT
    primary_artist,
    total_tracks,
    avg_popularity,
    peak_popularity,
    avg_danceability,
    avg_energy,
    avg_valence,
    primary_genre,
    RANK() OVER (ORDER BY avg_popularity DESC) AS popularity_rank
FROM aggregated
ORDER BY avg_popularity DESC
LIMIT 100
