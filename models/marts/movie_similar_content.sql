-- models/marts/movie_similar_content.sql

{{ config(materialized='table') }}

with base as (
    select *
    from {{ ref('features_movie_content') }}
    where primary_genre is not null
),

pairs as (
    select
        a.movie_id            as movie_id,
        a.title               as movie_title,
        b.movie_id            as similar_movie_id,
        b.title               as similar_movie_title,
        a.primary_genre,
        abs(a.release_year - b.release_year) as year_diff,
        b.vote_average,
        b.popularity
    from base a
    join base b
      on a.primary_genre = b.primary_genre   -- same genre
     and a.movie_id <> b.movie_id            -- exclude itself
)

select *
from pairs
qualify row_number() over (
    partition by movie_id
    order by vote_average desc, popularity desc, year_diff asc
) <= 10
