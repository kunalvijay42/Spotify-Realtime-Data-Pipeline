import json
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import boto3
import snowflake.connector
from dotenv import load_dotenv

# ------------------------------------------------------
#               LOAD ENVIRONMENT VARIABLES
# ------------------------------------------------------
load_dotenv(dotenv_path="/opt/airflow/dags/.env")

#               CONFIGURATION VARIABLES
# ------------------------------------------------------

# ----- MinIO Configuration -----
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = os.getenv("MINIO_BUCKET")
MINIO_PREFIX = os.getenv("MINIO_PREFIX")

# ----- Snowflake Configuration -----
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")
SNOWFLAKE_TABLE = os.getenv("SNOWFLAKE_TABLE")

# ----- Local File Path -----
LOCAL_TEMP_PATH = os.getenv("LOCAL_TEMP_PATH", "/tmp/spotify_raw.json")

# ------------------------------------------------------
#               PYTHON TASK FUNCTIONS
# ------------------------------------------------------

def extract_from_minio():
    """
    Extract all .json event files from MinIO -> combine -> save locally.
    """
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY
    )

    response = s3.list_objects_v2(Bucket=MINIO_BUCKET, Prefix=MINIO_PREFIX)
    contents = response.get("Contents", [])

    all_events = []
    for obj in contents:
        key = obj["Key"]
        if not key.endswith(".json"):
            continue

        data = s3.get_object(Bucket=MINIO_BUCKET, Key=key)
        lines = data["Body"].read().decode("utf-8").splitlines()

        for line in lines:
            try:
                all_events.append(json.loads(line))
            except json.JSONDecodeError:
                continue

    with open(LOCAL_TEMP_PATH, "w") as f:
        json.dump(all_events, f)

    print(f"✅ Extracted {len(all_events)} events from MinIO and saved to {LOCAL_TEMP_PATH}")
    return LOCAL_TEMP_PATH


def load_raw_to_snowflake(**context):
    """
    Load raw data into Snowflake Bronze table with idempotency.
    Uses MERGE statement to avoid duplicates based on event_id.
    """
    file_path = context["ti"].xcom_pull(task_ids="extract_data")

    with open(file_path, "r") as f:
        events = json.load(f)

    if not events:
        print("⚠️ No events found to load.")
        return

    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
    )
    cur = conn.cursor()

    # Set database and schema
    cur.execute(f"USE DATABASE {SNOWFLAKE_DATABASE}")
    cur.execute(f"USE SCHEMA {SNOWFLAKE_SCHEMA}")

    # Create main table if not exists
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {SNOWFLAKE_TABLE} (
        event_id STRING PRIMARY KEY,
        user_id STRING,
        song_id STRING,
        artist_name STRING,
        song_name STRING,
        event_type STRING,
        device_type STRING,
        country STRING,
        timestamp STRING
    );
    """
    cur.execute(create_table_sql)

    # Create temporary staging table
    staging_table = f"{SNOWFLAKE_TABLE}_STAGING"
    create_staging_sql = f"""
    CREATE TEMPORARY TABLE {staging_table} (
        event_id STRING,
        user_id STRING,
        song_id STRING,
        artist_name STRING,
        song_name STRING,
        event_type STRING,
        device_type STRING,
        country STRING,
        timestamp STRING
    );
    """
    cur.execute(create_staging_sql)

    # Insert all events into staging table
    insert_staging_sql = f"""
        INSERT INTO {staging_table} (
            event_id, user_id, song_id, artist_name, song_name,
            event_type, device_type, country, timestamp
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    for event in events:
        cur.execute(insert_staging_sql, (
            event.get("event_id"),
            event.get("user_id"),
            event.get("song_id"),
            event.get("artist_name"),
            event.get("song_name"),
            event.get("event_type"),
            event.get("device_type"),
            event.get("country"),
            event.get("timestamp")
        ))

    # Use MERGE to insert only new records (idempotent operation)
    merge_sql = f"""
    MERGE INTO {SNOWFLAKE_TABLE} AS target
    USING {staging_table} AS source
    ON target.event_id = source.event_id
    WHEN NOT MATCHED THEN
        INSERT (event_id, user_id, song_id, artist_name, song_name,
                event_type, device_type, country, timestamp)
        VALUES (source.event_id, source.user_id, source.song_id, source.artist_name,
                source.song_name, source.event_type, source.device_type,
                source.country, source.timestamp);
    """
    cur.execute(merge_sql)

    # Get count of newly inserted records
    cur.execute(f"SELECT COUNT(*) FROM {staging_table}")
    total_events = cur.fetchone()[0]
    
    cur.execute(f"""
        SELECT COUNT(*) FROM {staging_table} s
        WHERE NOT EXISTS (
            SELECT 1 FROM {SNOWFLAKE_TABLE} t
            WHERE t.event_id = s.event_id
        )
    """)
    new_events = cur.fetchone()[0]

    conn.commit()
    cur.close()
    conn.close()

    print(f"Processed {total_events} events from MinIO")
    print(f"Inserted {new_events} new records into Snowflake table: {SNOWFLAKE_TABLE}")
    print(f"ℹSkipped {total_events - new_events} duplicate records")


# ------------------------------------------------------
#               AIRFLOW DAG DEFINITION
# ------------------------------------------------------

default_args = {
    "owner": "airflow",
    "start_date": datetime(2026, 2, 13),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "spotify_minio_to_snowflake_bronze",
    default_args=default_args,
    description="Load raw Spotify events from MinIO to Snowflake Bronze table (idempotent)",
    schedule_interval="@hourly",
    catchup=False,
) as dag:

    extract_task = PythonOperator(
        task_id="extract_data",
        python_callable=extract_from_minio
    )

    load_task = PythonOperator(
        task_id="load_raw_to_snowflake",
        python_callable=load_raw_to_snowflake,
        provide_context=True
    )

    extract_task >> load_task