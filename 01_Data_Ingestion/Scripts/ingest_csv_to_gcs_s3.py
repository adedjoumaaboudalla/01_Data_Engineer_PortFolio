import pandas as pd
import os
import sys
import uuid
from google.cloud import storage
from Utils.helpers import error, info


# Ajoute le chemin vers le dossier parent de Utils
sys.path.append(os.path.abspath(os.path.join("..", "..")))



def save_df_to_gcs_bucket(df: pd.DataFrame, object_name = "csv/movies.csv") -> bool:
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
        client = storage.Client()  # nécessite GOOGLE_APPLICATION_CREDENTIALS
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(object_name + str(uuid.uuid4()))

        # Upload CSV
        blob.upload_from_string(df.to_csv(index=False), content_type="text/csv")
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
    save_df_to_gcs_bucket(df_movies_raw)

    info("Fin du traitement")