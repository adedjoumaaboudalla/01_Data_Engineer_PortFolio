from airflow import DAG
from datetime import datetime
import json
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import uuid
from airflow.exceptions import AirflowFailException
from airflow.models import Variable
from google.oauth2 import service_account
from google.cloud import storage
import pandas as pd




from p01_data_ingestion.scripts.ingest_api_to_db import getMovies, getAllPokemon
from p01_data_ingestion.scripts.ingest_csv_to_gcs_s3 import save_text_to_gcs_bucket, save_df_to_gcs_bucket
from Utils.helpers import info


def getAllPokemonMovies():
    pokemon_movies = getMovies(OMDB_API_KEY=Variable.get("OMDB_API_KEY"))
    info(str(pokemon_movies.head()))
    return pokemon_movies


def get_GCP_CREDENTIALS_SECRET():
    creds_json = Variable.get("GCP_CREDENTIALS_SECRET")
    creds_dict = json.loads(creds_json)
    return service_account.Credentials.from_service_account_info(creds_dict)

def process_pokemon_api_response(**context):
    # Récupérer la réponse de l'appel API à pokemon depuis XCom

    response_text = context['ti'].xcom_pull(task_ids='get_immo_data')
    info("Réponse brute de l'API :" + response_text)
    response_json = json.loads(response_text)
    text_content = json.dumps(response_json["results"], indent=2)


    if not save_text_to_gcs_bucket(text_content, "pokemon.txt", credentials=get_GCP_CREDENTIALS_SECRET()) :
        raise AirflowFailException("save_text_to_gcs_bucket failed")

def process_pokemon_movies_api_response(ti):
    # Récupérer la réponse de l'appel API à pokemon depuis XCom   
 
    df_movies = ti.xcom_pull(task_ids='get_moovies_task')
    info(df_movies)
    if not save_df_to_gcs_bucket(pd.DataFrame(df_movies), f"csv/movies_{uuid.uuid4()}.csv", credentials=get_GCP_CREDENTIALS_SECRET()):
        raise AirflowFailException("save_text_to_gcs_bucket failed")

# Creation de mon Dag
default_arguments = {
    "owner": "aad",
    "email": "adedjoumaaboudalla@gmail.com",
    "start_date": datetime(2025,9,1),
}


with DAG(dag_id="Pokemon_Story", default_args=default_arguments, schedule_interval="*0 0,12 * * *") as dag:
    
    # 0. Appel de la fonction getAllPokemonMovies
    get_moovies_task = PythonOperator(task_id= "get_moovies_task", python_callable= getAllPokemonMovies)

    # 1. Appel API avec HttpOperator
    get_pokemon_data = SimpleHttpOperator(
        task_id="get_immo_data",
        http_conn_id="poke_api",  # A configurer dans Airflow UI
        endpoint="v2/pokemon?limit=1500",
        method="GET",
        log_response=True,
        do_xcom_push=True,
    )

    # 2. Traitement de la réponse avec un PythonOperator
    process_pokemon_api_data = PythonOperator(
        task_id="process_pokemon_api_data",
        python_callable=process_pokemon_api_response,
        provide_context=True,
    )


    # 2. Traitement de la réponse avec un PythonOperator
    process_immo_pokemon_data = PythonOperator(
        task_id="process_immo_pokemon_data",
        python_callable=process_pokemon_movies_api_response,
        provide_context=True,
    )

    get_pokemon_data >> process_pokemon_api_data # type: ignore
    get_moovies_task >> process_immo_pokemon_data # type: ignore