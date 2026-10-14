-- dbt model: top_tracks_by_genre
-- Top 10 tracks per genre ranked by popularity score.
-- Deduplicates tracks that appear in multiple run dates (keeps latest).

WITH deduped AS (
    SELECT
        track_id,
        track_name,
        artists,
        genre,
        popularity,
        danceability,
        energy,
        valence,
        tempo,
        ROW_NUMBER() OVER (
            PARTITION BY track_id, genre
            ORDER BY date DESC
        ) AS rn
    FROM {{ source('spotify_analytics', 'raw_tracks') }}
),

latest AS (
    SELECT * FROM deduped WHERE rn = 1
),

ranked AS (
    SELECT
        genre,
        track_name,
        artists,
        popularity,
        danceability,
        energy,
        valence,
        tempo,
        RANK() OVER (
            PARTITION BY genre
            ORDER BY popularity DESC
        ) AS popularity_rank
    FROM latest
)

SELECT *
FROM ranked
WHERE popularity_rank <= 10
ORDER BY genre, popularity_rank
