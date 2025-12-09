-- models/staging/stg_tmdb_enriched.sql

{{ config(materialized='table') }}

select
    id                        as movie_id,
    title                     as api_title,
    runtime,
    status,
    popularity                as api_popularity,
    vote_average              as api_vote_average,
    vote_count                as api_vote_count,
    try_to_date(release_date) as api_release_date,
    original_lang,
    parse_json(genres)        as genres_json,          -- JSON string → VARIANT
    raw_payload               as raw_payload_json_str
from {{ source('raw', 'tmdb_movies_enriched') }}
