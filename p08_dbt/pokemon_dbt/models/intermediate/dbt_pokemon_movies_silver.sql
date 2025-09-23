
{{ config(materialized='view') }}

with ratings_avg as (
    SELECT avg(ratings) as new_ratings
    FROM {{ref("dbt_pokemon_movies")}}
    WHERE ratings is not null
)

select imdbID, Title, Released, Type, Country, IFNULL(ratings, ratings_avg.new_ratings) as ratings, TotalSeasons, pokemon_id
from {{ref("dbt_pokemon_movies")}}, ratings_avg
where 
imdbID is not null 
AND Country is not null

