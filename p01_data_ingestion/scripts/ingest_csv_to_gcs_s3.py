import pandas as pd
import os
import sys
import uuid
from google.cloud import storage
from Utils.helpers import error, info


# Ajoute le chemin vers le dossier parent de Utils
sys.path.append(os.path.abspath(os.path.join("..", "..")))



def save_df_to_gcs_bucket(df: pd.DataFrame, object_name: str, credentials=None) -> bool:
    """
    Stockage du DataFrame sur GCS

    Args:
        df(pd.DataFrame): DataFrame à stocker
        object_name(str): Object Name
    Returns
        bool
    """
    try:

        bucket_name = "de-01-data-ingestion"

        # Initialise client
        if credentials:
            client = storage.Client(credentials=credentials)
        else :
            client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(f"csv/{object_name}")

        # Upload CSV
        blob.upload_from_string(df.to_csv(index=False), content_type="text/csv")
        info("Upload réussi !")
    except Exception as e:
        error(f"File Creation failed with {e}")
        return False

    return True

def save_text_to_gcs_bucket(text: str, object_name, credentials=None) -> bool:
    """
    Stockage d'un contenue text sur GCS

    Args:
        df(pd.DataFrame): DataFrame à stocker
        object_name(str): Object Name
    Returns
        bool
    """
    try:

        bucket_name = "de-01-data-ingestion"

        # Initialise client
        if credentials:
            client = storage.Client(credentials=credentials)
        else :
            client = storage.Client()  # nécessite GOOGLE_APPLICATION_CREDENTIALS
        bucket = client.bucket(bucket_name)
        blob = bucket.blob("text/" + object_name)

        # Upload CSV
        blob.upload_from_string(text, content_type="text/plain")
        info("Upload réussi !")
    except Exception as e:
        error(f"File Creation failed with {e}")
        return False

    return True





if __name__ == "main":
    info("Strat process")

    # Extract All pokemon moovies
    df_movies_raw = pd.read_csv("../../00_assets/movies.csv")
    print(df_movies_raw.head())
    info("Extract Movies")

    # Save moovies in GCP
    object_name = "csv/movies.csv"
    save_df_to_gcs_bucket(df_movies_raw, object_name)

    info("Fin du traitement")