from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import requests
import json
import time

default_args = {
    "owner": "your_name",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def enrich_tmdb_movies(**context):
    """
    1. Connect to Snowflake.
    2. Get list of movie IDs from RAW.tmdb_movies_raw.
    3. Call TMDb API for each ID.
    4. Truncate and insert into RAW.tmdb_movies_enriched in batches of 1000.
    """
    tmdb_api_key = Variable.get("TMDB_API_KEY")

    hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
    conn = hook.get_conn()
    cursor = conn.cursor()

    try:
        cursor.execute("USE DATABASE USER_DB_GECKO;")
        cursor.execute("USE SCHEMA RAW;")

        # 1) Get distinct movie IDs
        cursor.execute("SELECT DISTINCT id FROM tmdb_movies_raw WHERE id IS NOT NULL;")
        rows = cursor.fetchall()
        movie_ids = [r[0] for r in rows]
        print(f"Found {len(movie_ids)} movie_ids to enrich")

        # 2) Full refresh enrichment table
        cursor.execute("TRUNCATE TABLE tmdb_movies_enriched;")

        base_url = "https://api.themoviedb.org/3/movie/"

        insert_sql = """
            INSERT INTO tmdb_movies_enriched (
                id,
                title,
                runtime,
                status,
                popularity,
                vote_average,
                vote_count,
                release_date,
                original_lang,
                genres,
                raw_payload
            )
            VALUES (
                %s,              -- id
                %s,              -- title
                %s,              -- runtime
                %s,              -- status
                %s,              -- popularity
                %s,              -- vote_average
                %s,              -- vote_count
                TO_DATE(%s),     -- release_date
                %s,              -- original_lang
                %s,  -- genres
                %s   -- raw_payload
            )
        """

        insert_values_batch = []
        batch_size = 1000

        for idx, movie_id in enumerate(movie_ids, start=1):
            url = f"{base_url}{movie_id}"
            params = {"api_key": tmdb_api_key}

            try:
                response = requests.get(url, params=params, timeout=10)
            except Exception as e:
                print(f"Request failed for movie_id={movie_id}: {e}")
                continue

            if response.status_code != 200:
                # e.g. 404 for some missing movie IDs – just log and skip
                print(f"TMDb API error for movie_id={movie_id}: status={response.status_code}")
                continue

            data = response.json()

            title = data.get("title")
            runtime = data.get("runtime")
            status = data.get("status")
            popularity = data.get("popularity")
            vote_average = data.get("vote_average")
            vote_count = data.get("vote_count")
            release_date = data.get("release_date")
            original_lang = data.get("original_language")
            genres = data.get("genres")

            insert_values_batch.append((
                movie_id,
                title,
                runtime,
                status,
                popularity,
                vote_average,
                vote_count,
                release_date,
                original_lang,
                json.dumps(genres),
                json.dumps(data),
            ))

            # Log every 1000 movies processed
            if idx % 1000 == 0:
                print(f"Processed {idx} movies so far")

            # When batch is full, insert to Snowflake
            if len(insert_values_batch) >= batch_size:
                cursor.executemany(insert_sql, insert_values_batch)
                conn.commit()
                print(f"Inserted batch of {len(insert_values_batch)} rows into tmdb_movies_enriched")
                insert_values_batch = []  # reset batch

            # Optional small sleep to be gentle on the API
            time.sleep(0.02)  # 20ms per call

        # Insert any remaining rows that didn't fill the last batch
        if insert_values_batch:
            cursor.executemany(insert_sql, insert_values_batch)
            conn.commit()
            print(f"Inserted final batch of {len(insert_values_batch)} rows into tmdb_movies_enriched")

    finally:
        cursor.close()
        conn.close()


with DAG(
    dag_id="movie_tmdb_etl_pipeline",
    default_args=default_args,
    description="ETL: Load TMDb 10000 movies CSV into Snowflake RAW schema",
    schedule_interval="@daily",      # or None / '@once' if you just want manual runs
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["movies", "tmdb", "snowflake", "etl"],
) as dag:


    create_raw_objects = SnowflakeOperator(
        task_id="create_raw_objects",
        snowflake_conn_id="snowflake_conn",
        sql="""
        -- Ensure compute is available

        USE WAREHOUSE GECKO_QUERY_WH;
        USE DATABASE USER_DB_GECKO;
        USE SCHEMA RAW;

        -- Reusable CSV file format
        CREATE OR REPLACE FILE FORMAT RAW.CSV_FORMAT
          TYPE = 'CSV'
          FIELD_DELIMITER = ','
          SKIP_HEADER = 1
          FIELD_OPTIONALLY_ENCLOSED_BY = '"'
          NULL_IF = ('', 'NULL', 'null');

        -- Internal stage for movie-related files
        CREATE STAGE IF NOT EXISTS RAW.MOVIE_STAGE
          FILE_FORMAT = RAW.CSV_FORMAT;

        -- Raw TMDb movies table matching the CSV structure
        CREATE OR REPLACE TABLE RAW.tmdb_movies_raw (
            id                INTEGER,
            original_language STRING,
            original_title    STRING,
            overview          STRING,
            popularity        FLOAT,
            release_date      DATE,
            title             STRING,
            vote_average      FLOAT,
            vote_count        INTEGER
        );
        """,
    )

    # 2) Load data from the staged CSV into tmdb_movies_raw.
    #    Assumes you've uploaded `tmdb_10000_movies.csv` to RAW.MOVIE_STAGE.
    load_tmdb_movies = SnowflakeOperator(
        task_id="load_tmdb_movies",
        snowflake_conn_id="snowflake_conn",
        sql="""
        USE WAREHOUSE GECKO_QUERY_WH ;
        USE DATABASE USER_DB_GECKO;
        USE SCHEMA RAW;

        -- Optional: truncate old data before reloading (full refresh)
        TRUNCATE TABLE RAW.tmdb_movies_raw;

        COPY INTO RAW.tmdb_movies_raw
        FROM @RAW.MOVIE_STAGE/tmdb_10000_movies.csv
        FILE_FORMAT = (FORMAT_NAME = RAW.CSV_FORMAT)
        ON_ERROR = 'CONTINUE';
        """,
    )

    # 3) Simple validation: log row count for sanity
    validate_tmdb_load = SnowflakeOperator(
        task_id="validate_tmdb_load",
        snowflake_conn_id="snowflake_conn",
        sql="""
        USE DATABASE USER_DB_GECKO;
        USE SCHEMA RAW;

        SELECT COUNT(*) AS movie_count
        FROM RAW.tmdb_movies_raw;
        """,
    )
    enrich_with_tmdb = PythonOperator(
        task_id="enrich_with_tmdb",
        python_callable=enrich_tmdb_movies,
        provide_context=True,
    )


    # Task dependencies: create objects -> load -> validate
    create_raw_objects >> load_tmdb_movies >> validate_tmdb_load >> enrich_with_tmdb
