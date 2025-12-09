-- models/core/dim_movie.sql

{{ config(materialized='table') }}

with movies as (
    select * from {{ ref('stg_tmdb_movies') }}
),
enriched as (
    select * from {{ ref('stg_tmdb_enriched') }}
)

select
    m.movie_id,
    coalesce(e.api_title, m.title)                   as title,
    m.original_title,
    coalesce(e.original_lang, m.original_language)   as original_language,
    e.runtime,
    e.status,
    coalesce(e.api_popularity, m.popularity)         as popularity,
    coalesce(e.api_vote_average, m.vote_average)     as vote_average,
    coalesce(e.api_vote_count, m.vote_count)         as vote_count,
    m.release_date,
    m.release_year,
    m.overview,
    e.genres_json
from movies m
left join enriched e
  on m.movie_id = e.movie_id
