{{ config(materialized='table') }}

with source_data as (

    select imdbID, Title, Released, Type, Country, Ratings, TotalSeasons, pokemon_id
    FROM pokemon_ds.pokemon_movies

)

select *
from source_data
where imdbID is not null
