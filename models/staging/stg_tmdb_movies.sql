-- models/staging/stg_tmdb_movies.sql

{{ config(materialized='table') }}

select
    id                              as movie_id,
    title,
    original_title,
    original_language,
    try_to_date(release_date)       as release_date,
    year(try_to_date(release_date)) as release_year,
    overview,
    popularity,
    vote_average,
    vote_count
from {{ source('raw', 'tmdb_movies_raw') }}
