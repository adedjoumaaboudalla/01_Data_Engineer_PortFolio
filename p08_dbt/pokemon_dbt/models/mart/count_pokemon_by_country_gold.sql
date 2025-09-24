{{ config(materialized='table', schema='dbt_pokemon') }}

SELECT Country, COUNT(*) as movies_count
from {{ref("pokemon_movies_silver")}}
GROUP BY Country