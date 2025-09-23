{{ config(materialized='view') }}

select *
from {{ref("dbt_pokemon_movies_silver")}}
where 
pokemon_id is not null 