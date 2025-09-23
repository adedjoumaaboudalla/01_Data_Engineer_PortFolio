# 📊 DBT pokemon

Ce dossier illustre comment mettre en place une **Architechture medaillon avec DBT**.

## Contenu
- **models/**
  - `dbt_pokemon_movies.sql` → Extraction de tous les films pokemon.
  - `dbt_pokemon_movies_silver.sql` → Nettoyage de la liste des films.
  - `dbt_pokemon_movies_clean_pokemon_id.sql` → Liste des film liés à un pokemon
  - `dbt_count_pokemon_by_country_gold.sql` → Agregation des film par pays.

## Compétences mises en avant
- Creation d'un projet.
- Connexion à bigquery
- Création de models
- Execution de model
- Execution des tests
