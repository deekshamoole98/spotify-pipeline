-- dbt model: popularity_vs_audio_features
-- Buckets tracks by popularity tier and computes average audio features
-- per tier — answers "do high-popularity songs sound different?"
-- Useful for scatter plots and correlation analysis in dashboards.

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
        acousticness,
        instrumentalness,
        tempo,
        loudness,
        explicit,
        ROW_NUMBER() OVER (
            PARTITION BY track_id, genre
            ORDER BY date DESC
        ) AS rn
    FROM {{ source('spotify_analytics', 'raw_tracks') }}
),

latest AS (SELECT * FROM deduped WHERE rn = 1),

tiered AS (
    SELECT
        *,
        CASE
            WHEN popularity >= 80 THEN 'viral'        -- top tier
            WHEN popularity >= 60 THEN 'popular'
            WHEN popularity >= 40 THEN 'mid'
            WHEN popularity >= 20 THEN 'niche'
            ELSE 'obscure'
        END AS popularity_tier
    FROM latest
)

SELECT
    popularity_tier,
    genre,
    COUNT(*)                          AS track_count,
    ROUND(AVG(popularity), 1)         AS avg_popularity,
    ROUND(AVG(danceability), 3)       AS avg_danceability,
    ROUND(AVG(energy), 3)             AS avg_energy,
    ROUND(AVG(valence), 3)            AS avg_valence,
    ROUND(AVG(acousticness), 3)       AS avg_acousticness,
    ROUND(AVG(tempo), 1)              AS avg_tempo_bpm,
    ROUND(AVG(loudness), 2)           AS avg_loudness_db,
    ROUND(AVG(CAST(explicit AS INT)), 3) AS explicit_rate
FROM tiered
GROUP BY popularity_tier, genre
ORDER BY
    CASE popularity_tier
        WHEN 'viral'   THEN 1
        WHEN 'popular' THEN 2
        WHEN 'mid'     THEN 3
        WHEN 'niche'   THEN 4
        ELSE 5
    END,
    genre
