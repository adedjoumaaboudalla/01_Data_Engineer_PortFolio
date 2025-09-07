# ⏱️ Data Orchestration

Ce dossier contient des exemples d’orchestration et d’automatisation de pipelines.

## Contenu
- **airflow/dags/**
  - `etl_pipeline.py` → DAG Airflow pour orchestrer un pipeline ETL 2 fois dans la journée
      - Chargement des données des film Pokemon via Api 
      - Chargement de la liste des pokemons via Api
      - Satockage des films en csv sur GCS
      - Stockage de la liste des pokemons en text sur GCS
- **prefect/**
  - `etl_flow.py` → flow Prefect pour orchestrer un pipeline.

## Compétences mises en avant
- Automatisation de pipelines.
- Orchestration avec Airflow et Prefect.
- Gestion des dépendances et planification.
