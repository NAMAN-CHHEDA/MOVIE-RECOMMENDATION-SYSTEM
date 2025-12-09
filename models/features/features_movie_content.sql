-- models/features/features_movie_content.sql

{{ config(materialized='table') }}

select
    movie_id,
    title,
    original_language,
    release_year,
    runtime,
    popularity,
    vote_average,
    vote_count,

    -- NEW: fill missing genre with 'Unknown'
    coalesce(genres_json[0]:name::string, 'Unknown') as primary_genre,

    case
        when runtime is null then 'unknown'
        when runtime < 80 then 'short'
        when runtime between 80 and 120 then 'medium'
        else 'long'
    end as runtime_bucket
from {{ ref('dim_movie') }}
