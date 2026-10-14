# Spotify Analytics Pipeline

An end-to-end data engineering pipeline that ingests a 114K-track Spotify dataset, stores it as a partitioned data lake on AWS S3, and surfaces analytics through Athena + dbt models.

Built as a portfolio project demonstrating the BI engineering skills used in production analytics infrastructure: batch ingestion, data quality validation, dimensional modeling, and self-serve analytics.

---

## Architecture

```
Spotify Dataset (CSV)
        ↓
  ingest.py (Python)
  ├── Schema validation
  ├── Deduplication
  └── Data quality checks (pass rate, invalid rows, duplicates)
        ↓
  S3 Data Lake
  raw/date=YYYY-MM-DD/genre=<genre>/tracks.json   ← NDJSON, partitioned
  raw/date=YYYY-MM-DD/manifest.json               ← DQ run summary
        ↓
  AWS Athena (external table, JsonSerDe)
        ↓
  dbt (4 analytical models as Athena views)
        ↓
  Amazon QuickSight / Dashboard
```

---

## Dataset

[Spotify Tracks Dataset](https://www.kaggle.com/datasets/maharshipandya/-spotify-tracks-dataset) — 114K tracks across 114 genres with audio features: danceability, energy, valence, tempo, loudness, acousticness, instrumentalness, liveness, speechiness.

---

## Data Quality

Every ingestion run validates:

| Check | What it catches |
|---|---|
| Required field presence | Missing track_id, genre, audio features |
| Numeric type validation | Non-numeric values in float/int fields |
| Deduplication | Same track_id + genre appearing multiple times |
| Pass rate | % valid rows logged to manifest.json |

Run stats on this dataset: **99.6% pass rate**, 113,549 valid rows, 450 duplicates dropped.

---

## dbt Models

| Model | Description |
|---|---|
| `top_tracks_by_genre` | Top 10 tracks per genre ranked by popularity |
| `audio_features_by_genre` | Avg danceability, energy, valence, tempo per genre — sonic fingerprint |
| `artist_popularity` | Top 100 artists by avg track popularity (min 3 tracks) |
| `popularity_vs_audio_features` | Audio features bucketed by popularity tier (viral → obscure) |

---

## Project Structure

```
spotify_pipeline/
├── ingestion/
│   └── ingest.py           # Batch ingestion + DQ + S3 writer
├── db/
│   └── athena_schema.sql   # External table DDL
├── dbt/
│   ├── dbt_project.yml
│   └── models/
│       ├── schema.yml
│       ├── top_tracks_by_genre.sql
│       ├── audio_features_by_genre.sql
│       ├── artist_popularity.sql
│       └── popularity_vs_audio_features.sql
├── infra/iam/
│   ├── lambda_trust_policy.json
│   └── lambda_permission_policy.json
├── .env.example
├── requirements.txt
└── .gitignore
```

---

## Running Locally

```bash
# Install dependencies
pip install -r requirements.txt

# Copy env file and set values
cp .env.example .env

# Run ingestion locally (no AWS needed)
STORAGE_BACKEND=local SOURCE_CSV=dataset.csv python ingestion/ingest.py
```

Output lands in `data/date=<today>/genre=<genre>/tracks.json`.

## Running on AWS

1. Create S3 bucket and update `S3_BUCKET` in `.env`
2. Create IAM role using policies in `infra/iam/`
3. Deploy `ingest.py` as a Lambda function
4. Set environment variables on the Lambda
5. Schedule with EventBridge (daily or weekly)
6. Run `athena_schema.sql` in Athena to create the external table
7. Run `MSCK REPAIR TABLE` after first load to register partitions
8. Run dbt models: `dbt run --profiles-dir .`

---

## Key Design Decisions

**Partitioned by date + genre** — enables Athena partition pruning; queries scoped to a genre scan only that partition rather than the full dataset.

**NDJSON over CSV** — Athena's JsonSerDe handles malformed rows gracefully (`ignore.malformed.json=TRUE`) without failing the entire query.

**`payload`-style storage** — audio features stored as flat fields (not nested) for direct Athena column access without `json_extract`.

**dbt as views** — models are lightweight Athena views rather than materialized tables; avoids storage cost for a portfolio dataset while keeping the transformation layer explicit and testable.
