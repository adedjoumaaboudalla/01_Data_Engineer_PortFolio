{{ config(materialized='table') }}

SELECT Country, COUNT(*) as movies_count
from {{ref("dbt_pokemon_movies_silver")}}
GROUP BY Country