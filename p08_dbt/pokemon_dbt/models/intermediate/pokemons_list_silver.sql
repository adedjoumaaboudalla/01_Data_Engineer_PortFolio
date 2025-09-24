
{{ config(materialized='view', schema='dbt_pokemon') }}


SELECT
  pokemons.base_experience,
  pokemons.height,
  pokemons.is_default,
  pokemons.location_area_encounters,
  pokemons.name,
  pokemons.order,
  pokemons.weight,
  pokemons.id,
  pokemons.cries,
  pokemon_ds.get_spicies_names(pokemons.species) AS spicies,
  pokemon_ds.get_ability_names(abilities) AS abilities,
  pokemon_ds.get_forms_names(pokemons.forms) AS forms,
  pokemon_ds.get_held_items_names(pokemons.held_items) AS items
FROM {{ref("pokemon_list")}} AS pokemons

