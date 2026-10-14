-- dbt model: audio_features_by_genre
-- Average audio features per genre — useful for radar/spider charts
-- showing what makes each genre sonically distinct.

WITH deduped AS (
    SELECT
        track_id,
        genre,
        danceability,
        energy,
        valence,
        acousticness,
        instrumentalness,
        liveness,
        speechiness,
        loudness,
        tempo,
        popularity,
        ROW_NUMBER() OVER (
            PARTITION BY track_id, genre
            ORDER BY date DESC
        ) AS rn
    FROM {{ source('spotify_analytics', 'raw_tracks') }}
),

latest AS (SELECT * FROM deduped WHERE rn = 1)

SELECT
    genre,
    COUNT(*)                          AS track_count,
    ROUND(AVG(danceability), 3)       AS avg_danceability,
    ROUND(AVG(energy), 3)             AS avg_energy,
    ROUND(AVG(valence), 3)            AS avg_valence,
    ROUND(AVG(acousticness), 3)       AS avg_acousticness,
    ROUND(AVG(instrumentalness), 3)   AS avg_instrumentalness,
    ROUND(AVG(liveness), 3)           AS avg_liveness,
    ROUND(AVG(speechiness), 3)        AS avg_speechiness,
    ROUND(AVG(loudness), 2)           AS avg_loudness_db,
    ROUND(AVG(tempo), 1)              AS avg_tempo_bpm,
    ROUND(AVG(popularity), 1)         AS avg_popularity
FROM latest
GROUP BY genre
ORDER BY track_count DESC
