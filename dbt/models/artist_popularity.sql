-- dbt model: artist_popularity
-- Top artists by average track popularity and total track count.
-- Handles multi-artist tracks (semicolon-separated) by attributing
-- popularity to the first listed artist.

-- dbt model: artist_popularity
WITH deduped AS (
    SELECT
        track_id,
        TRIM(SPLIT_PART(artists, ';', 1)) AS primary_artist,
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

genre_counts AS (
    SELECT
        primary_artist,
        genre,
        COUNT(*) AS genre_count
    FROM latest
    GROUP BY primary_artist, genre
),

primary_genres AS (
    SELECT
        primary_artist,
        MAX_BY(genre, genre_count) AS primary_genre
    FROM genre_counts
    GROUP BY primary_artist
),

aggregated AS (
    SELECT
        l.primary_artist,
        COUNT(DISTINCT l.track_id)        AS total_tracks,
        ROUND(AVG(l.popularity), 1)       AS avg_popularity,
        MAX(l.popularity)                 AS peak_popularity,
        ROUND(AVG(l.danceability), 3)     AS avg_danceability,
        ROUND(AVG(l.energy), 3)           AS avg_energy,
        ROUND(AVG(l.valence), 3)          AS avg_valence,
        pg.primary_genre
    FROM latest l
    JOIN primary_genres pg ON l.primary_artist = pg.primary_artist
    GROUP BY l.primary_artist, pg.primary_genre
    HAVING COUNT(DISTINCT l.track_id) >= 3
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
