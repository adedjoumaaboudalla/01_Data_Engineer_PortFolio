{{ config(materialized='ephemeral', schema='dbt_pokemon') }}

select *
from {{source('pokemon_ds', 'pokemons')}}
where id is not null
