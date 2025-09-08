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
from airflow.providers.google.cloud.sensors.gcs import GCSObjectUpdateSensor
from datetime import datetime, timedelta

import pandas as pd




from p01_data_ingestion.scripts.ingest_api_to_db import getMovies, getAllPokemon
from p01_data_ingestion.scripts.ingest_csv_to_gcs_s3 import save_text_to_gcs_bucket, save_df_to_gcs_bucket
from Utils.helpers import info



def one_hour_ago(context):    
    info(f"Context: {context}")
    return datetime.now() - timedelta(hours=1)


def getAllPokemonMovies():
    """
    Récupere Tous les films pokemon  sur omnidb
    """
    pokemon_movies = getMovies(OMDB_API_KEY=Variable.get("OMDB_API_KEY"))
    info(str(pokemon_movies.head()))
    return pokemon_movies


def get_GCP_CREDENTIALS_SECRET():
    """
    Retourne le credential de GCP
    """
    creds_json = Variable.get("GCP_CREDENTIALS_SECRET")
    creds_dict = json.loads(creds_json)
    return service_account.Credentials.from_service_account_info(creds_dict)

def process_pokemon_api_response(**context):
    # Récupérer la réponse de l'appel API à pokemon depuis XCom

    response_text = context['ti'].xcom_pull(task_ids='get_immo_data')
    info("Réponse brute de l'API :" + response_text)
    response_json = json.loads(response_text)
    text_content = json.dumps(response_json["results"], indent=2)

    object_pokemon_list_name = f"pokemon_{uuid.uuid4()}.txt"

    Variable.set("latest_object_pokemon_list_name", object_pokemon_list_name)
    
    info(f"object_pokemon_list_name={object_pokemon_list_name}")

    if not save_text_to_gcs_bucket(text_content, object_pokemon_list_name, credentials=get_GCP_CREDENTIALS_SECRET()) :
        raise AirflowFailException("save_text_to_gcs_bucket failed")

def process_pokemon_movies_api_response(ti):
    # Récupérer la réponse de l'appel API à pokemon depuis XCom   
    df_movies = ti.xcom_pull(task_ids='get_moovies_task')

    object_movies_name = f"movies_{uuid.uuid4()}.csv"
    Variable.set("latest_object_movies_name", object_movies_name)

    info(f"object_movies_name={object_movies_name}")

    if not save_df_to_gcs_bucket(pd.DataFrame(df_movies), object_name=object_movies_name, credentials=get_GCP_CREDENTIALS_SECRET()):
        raise AirflowFailException("save_text_to_gcs_bucket failed")

# Creation de mon Dag
default_arguments = {
    "owner": "aad",
    "email": "adedjoumaaboudalla@gmail.com",
    "start_date": datetime(2025,9,1),
}


with DAG(dag_id="Pokemon_Story", default_args=default_arguments, schedule_interval="0 0,12 * * *") as dag:
    
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

    # 2. Traitement de la réponse process_pokemon_api_response avec un PythonOperator
    process_pokemon_api_data = PythonOperator(
        task_id="process_pokemon_api_data",
        python_callable=process_pokemon_api_response,
        provide_context=True,
    )


    # 3. Traitement de la réponse process_pokemon_movies_api_response avec un PythonOperator
    process_immo_pokemon_data = PythonOperator(
        task_id="process_immo_pokemon_data",
        python_callable=process_pokemon_movies_api_response,
        provide_context=True,
    )

    
    # 4. Verification de l'existance ou la mise à jour des fichiers pokemon'
    check_movies_pokemon_gcs_update = GCSObjectUpdateSensor(
        task_id='check_movies_pokemon_gcs_update',
        bucket='de-01-data-ingestion',
        object=f"csv/{Variable.get('latest_object_movies_name', '')}",   
        ts_func=one_hour_ago,
        google_cloud_conn_id='google_cloud_default',
        poke_interval=60,
        timeout=1800,
        mode='poke',
        dag=dag
    )

    check_pokemon_list_gcs_update = GCSObjectUpdateSensor(
        task_id='check_pokemon_list_gcs_update',
        bucket='de-01-data-ingestion',        
        object=f"text/{Variable.get('latest_object_pokemon_list_name', '')}",   
        ts_func=one_hour_ago,
        google_cloud_conn_id='google_cloud_default',
        poke_interval=60,
        timeout=1800,
        mode='poke',
        dag=dag
    )


    get_pokemon_data >> process_pokemon_api_data >> check_pokemon_list_gcs_update # type: ignore
    get_moovies_task >> process_immo_pokemon_data >> check_movies_pokemon_gcs_update # type: ignore