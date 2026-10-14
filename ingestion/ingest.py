"""
spotify_pipeline/ingestion/ingest.py

Reads the Spotify tracks CSV, validates it, partitions by genre,
and writes date-partitioned NDJSON to S3 (or local disk for testing).

Environment variables:
    S3_BUCKET        Target S3 bucket name
    STORAGE_BACKEND  "s3" (default) or "local"
    LOCAL_DATA_DIR   Output dir when STORAGE_BACKEND=local (default: data/)
    SOURCE_CSV       Path to dataset.csv (default: dataset.csv)
"""

import csv
import json
import logging
import os
import io
from datetime import date, datetime
from collections import defaultdict

import boto3
from botocore.exceptions import BotoCoreError, ClientError

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
S3_BUCKET       = os.environ.get("S3_BUCKET", "YOUR_BUCKET_NAME")
STORAGE_BACKEND = os.environ.get("STORAGE_BACKEND", "local")
LOCAL_DATA_DIR  = os.environ.get("LOCAL_DATA_DIR", "data")
SOURCE_CSV      = os.environ.get("SOURCE_CSV", "dataset.csv")
RUN_DATE        = os.environ.get("RUN_DATE", date.today().isoformat())  # YYYY-MM-DD

# ── Schema ────────────────────────────────────────────────────────────────────
REQUIRED_FIELDS = {
    "track_id", "artists", "track_name", "track_genre",
    "popularity", "danceability", "energy", "valence",
    "tempo", "loudness", "acousticness", "instrumentalness",
    "liveness", "speechiness", "duration_ms", "explicit",
}

NUMERIC_FIELDS = {
    "popularity", "duration_ms", "danceability", "energy",
    "loudness", "speechiness", "acousticness", "instrumentalness",
    "liveness", "valence", "tempo",
}


# ── Data quality ──────────────────────────────────────────────────────────────
def validate_row(row: dict, row_num: int) -> tuple[dict | None, list[str]]:
    """Return (cleaned_row, issues). cleaned_row is None if row is unfixable."""
    issues = []

    # Check required fields present and non-empty
    for f in REQUIRED_FIELDS:
        if f not in row or row[f].strip() == "":
            issues.append(f"row {row_num}: missing field '{f}'")

    if issues:
        return None, issues

    # Cast numeric fields
    cleaned = dict(row)
    for f in NUMERIC_FIELDS:
        try:
            cleaned[f] = float(cleaned[f])
        except ValueError:
            issues.append(f"row {row_num}: non-numeric value in '{f}': {cleaned[f]!r}")
            return None, issues

    cleaned["popularity"] = int(cleaned["popularity"])
    cleaned["duration_ms"] = int(cleaned["duration_ms"])
    cleaned["explicit"] = cleaned["explicit"].strip().lower() == "true"
    cleaned["ingested_at"] = datetime.utcnow().isoformat() + "Z"
    cleaned["run_date"] = RUN_DATE

    return cleaned, []


# ── I/O ───────────────────────────────────────────────────────────────────────
def load_csv(path: str) -> tuple[list[dict], dict]:
    """Load and validate CSV. Returns (valid_rows, dq_summary)."""
    valid_rows = []
    all_issues = []
    seen_ids = set()
    duplicate_count = 0

    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader, start=2):  # row 1 = header
            # Dedup by track_id + genre (same track can appear in multiple genres)
            key = (row.get("track_id", ""), row.get("track_genre", ""))
            if key in seen_ids:
                duplicate_count += 1
                continue
            seen_ids.add(key)

            cleaned, issues = validate_row(row, i)
            if cleaned:
                valid_rows.append(cleaned)
            else:
                all_issues.extend(issues)

    total = len(valid_rows) + len(all_issues) + duplicate_count
    dq_summary = {
        "run_date": RUN_DATE,
        "total_rows_read": total,
        "valid_rows": len(valid_rows),
        "invalid_rows": len(all_issues),
        "duplicate_rows": duplicate_count,
        "pass_rate_pct": round(len(valid_rows) / total * 100, 2) if total else 0,
        "sample_issues": all_issues[:10],
    }
    return valid_rows, dq_summary


def partition_by_genre(rows: list[dict]) -> dict[str, list[dict]]:
    """Group rows by track_genre for partitioned S3 writes."""
    buckets = defaultdict(list)
    for row in rows:
        genre = row["track_genre"].lower().replace(" ", "_").replace("/", "_")
        buckets[genre].append(row)
    return buckets


def rows_to_ndjson(rows: list[dict]) -> str:
    return "\n".join(json.dumps(r) for r in rows)


# ── Writers ───────────────────────────────────────────────────────────────────
def write_local(genre: str, content: str, run_date: str):
    path = os.path.join(LOCAL_DATA_DIR, f"date={run_date}", f"genre={genre}", "tracks.json")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        f.write(content)
    log.info(f"[local] wrote {path}")


def write_s3(genre: str, content: str, run_date: str):
    key = f"raw/date={run_date}/genre={genre}/tracks.json"
    s3 = boto3.client("s3")
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=content.encode("utf-8"),
        ContentType="application/x-ndjson",
    )
    log.info(f"[s3] uploaded s3://{S3_BUCKET}/{key}")


def write_manifest(dq_summary: dict, run_date: str):
    content = json.dumps(dq_summary, indent=2)
    if STORAGE_BACKEND == "s3":
        key = f"raw/date={run_date}/manifest.json"
        boto3.client("s3").put_object(
            Bucket=S3_BUCKET, Key=key,
            Body=content.encode(), ContentType="application/json",
        )
        log.info(f"[s3] manifest → s3://{S3_BUCKET}/{key}")
    else:
        path = os.path.join(LOCAL_DATA_DIR, f"date={run_date}", "manifest.json")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as f:
            f.write(content)
        log.info(f"[local] manifest → {path}")


# ── Main ──────────────────────────────────────────────────────────────────────
def run():
    log.info(f"Loading CSV from {SOURCE_CSV}")
    rows, dq_summary = load_csv(SOURCE_CSV)

    log.info(
        f"DQ summary: {dq_summary['valid_rows']} valid, "
        f"{dq_summary['invalid_rows']} invalid, "
        f"{dq_summary['duplicate_rows']} duplicates, "
        f"pass rate {dq_summary['pass_rate_pct']}%"
    )

    partitions = partition_by_genre(rows)
    log.info(f"Writing {len(partitions)} genre partitions for date={RUN_DATE}")

    for genre, genre_rows in partitions.items():
        ndjson = rows_to_ndjson(genre_rows)
        if STORAGE_BACKEND == "s3":
            write_s3(genre, ndjson, RUN_DATE)
        else:
            write_local(genre, ndjson, RUN_DATE)

    write_manifest(dq_summary, RUN_DATE)
    log.info("Ingestion complete.")
    return dq_summary


# Lambda entrypoint
def lambda_handler(event, context):
    summary = run()
    return {"statusCode": 200, "body": json.dumps(summary)}


if __name__ == "__main__":
    run()
