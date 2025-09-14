# 🔄 Data Transformation

Ce dossier contient des pipelines de **transformation et nettoyage des données**.

## Contenu
- **scripts/**
  - `transform_etl.py` → script d’ETL pour enrichir les données des films pokemons et insertion dans bigquery.
- **notebooks/**
  - `transformation_demo.ipynb` → démonstration des étapes de transformation avec Pandas ou PySpark.

## Compétences mises en avant
- Nettoyage de données.
- Transformation et enrichissement.
- AJout des données dans BigQuery
- Pipelines ETL (Extract – Transform – Load).

## Points importants
- Ces variables d'environnement sont importants à definir pour le bon fonctionnement
  - GCP_CREDENTIALS_SECRET, PYSPARK_PYTHON, PYSPARK_DRIVER_PYTHON, HADOODP_HOME, OMDB_API_KEY
- Veiller à télécharger le gcs-connector-hadoop3-2.2.2-shaded.jar dans le dossier libs
