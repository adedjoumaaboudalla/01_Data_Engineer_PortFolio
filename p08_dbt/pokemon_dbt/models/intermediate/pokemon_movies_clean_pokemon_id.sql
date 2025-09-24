{{ config(materialized='view', schema='dbt_pokemon') }}

select *
from {{ref("pokemon_movies_silver")}}
where 
pokemon_id is not null 