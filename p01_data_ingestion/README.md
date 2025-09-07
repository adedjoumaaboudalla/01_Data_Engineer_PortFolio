# 📥 Data Ingestion

Ce dossier contient des projets et scripts liés à **l’ingestion de données** depuis différentes sources.

## Contenu
- **scripts/**
  - `ingest_csv_to_s3.py` → script pour ingérer des fichiers CSV et stocker les données brutes dans S3 et GCS.
  - `ingest_api_to_db.py` → script pour consommer une API vers une base de données relationnelle (SQLite).
- **notebooks/**
  - `ingestion_demo.ipynb` → démonstration interactive du processus d’ingestion.

### Prerequis
  - Ajouter votre clé GOOGLE_APPLICATION_CREDENTIALS comme suit
    - Windows : setx GOOGLE_APPLICATION_CREDENTIALS "Chemin_vers_la_cle_json"
    - Mac / Linux :export GOOGLE_APPLICATION_CREDENTIALS="Chemin_vers_la_cle_json"

  - Ajouter votre clé omdbapi comme suit
    - Windows : setx OMDB_API_KEY "votre_secret"
    - Mac / Linux :export OMDB_API_KEY="votre_secret"

## Compétences mises en avant
- Lecture et ingestion de données (CSV, API).
- Connexion à une base de données.
- Écriture dans des systèmes de stockage (S3, GCS).
