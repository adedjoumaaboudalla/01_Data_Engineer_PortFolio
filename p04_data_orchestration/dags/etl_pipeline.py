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
import os

from p01_data_ingestion.scripts.ingest_api_to_db import getMovies
from p01_data_ingestion.scripts.ingest_csv_to_gcs_s3 import save_text_to_gcs_bucket, save_df_to_gcs_bucket

from p02_data_transformation.Scripts.transform_etl import transform_pokemon, get_spark_config, get_pokemon_movies_schema, pokemon_movies_cleaning, save_list_on_bigquery, get_pokemons_schema, pokemons_cleaning

from Utils.helpers import info



debug_path = "/opt/airflow/debug"
os.makedirs(debug_path, exist_ok=True)

import numpy as np

def make_jsonable(x):
    if isinstance(x, np.ndarray):
        return x.tolist()
    elif isinstance(x, dict):
        return {k: make_jsonable(v) for k, v in x.items()}
    elif isinstance(x, list):
        return [make_jsonable(v) for v in x]
    else:
        return x


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
    

def transform_pokemon_data():
    info("Start process")

    creds_json = os.getenv("GCP_CREDENTIALS_SECRET")
    import logging
    logging.info(f"Used creds_json = {creds_json}")

    spark = get_spark_config("/opt/airflow/libs/gcs-connector-hadoop3-2.2.2-shaded.jar")

    datas = transform_pokemon(spark)
    info(f"Transform Data ({len(datas[0])}, {len(datas[1])})")
    return datas

def save_pokemons_movies(**context):
    #Save movies on BigQuery
    datas = context['ti'].xcom_pull(task_ids='process_data_enrichment')
    
    pokemon_movies_schema = get_pokemon_movies_schema(datas[0])

    pokemons_movies = pokemon_movies_cleaning(datas[0])

    if not save_list_on_bigquery(data=pokemons_movies, my_table="dataengineer-471201.pokemon_ds.pokemon_movies", schema=pokemon_movies_schema):
       raise AirflowFailException("save_pokemon_movies_on_bigquery failed")
    info("Save pokemons movies")

def save_pokemons(**context):
    #Save pokemon list on BigQuery
    datas = context['ti'].xcom_pull(task_ids='process_data_enrichment')

    pokemon_schema = get_pokemons_schema(datas[1])

    pokemons = pokemons_cleaning(datas[1])
    if not save_list_on_bigquery(data=pokemons , my_table="dataengineer-471201.pokemon_ds.pokemons", schema=pokemon_schema):
        raise AirflowFailException("save_pokemon_on_bigquery failed")
    info("Process finished")

# Creation de mon Dag
default_arguments = {
    "owner": "aad",
    "email": "adedjoumaaboudalla@gmail.com",
    "start_date": datetime(2025,9,1),
}


with DAG(dag_id="Pokemon_Story", default_args=default_arguments, schedule_interval="0 0,12 * * *") as dag:
    
    # 1. Appel de la fonction getAllPokemonMovies
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

    # 3. Enrichissement des données
    process_data_enrichment = PythonOperator(
        task_id="process_data_enrichment",
        python_callable=transform_pokemon_data,
        provide_context=True,
    )

    # 4. Enrichissement des données
    process_save_pokemons = PythonOperator(
        task_id="process_save_pokemons",
        python_callable=save_pokemons,
        provide_context=True,
    )

    # 4. Enrichissement des données
    process_save_pokemons_movies = PythonOperator(
        task_id="process_save_pokemons_movies",
        python_callable=save_pokemons_movies,
        provide_context=True,
    )

    [get_pokemon_data >> process_pokemon_api_data, get_moovies_task >> process_immo_pokemon_data]  >> process_data_enrichment >> [process_save_pokemons_movies, process_save_pokemons] # type: ignore
     