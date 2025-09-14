import os, sys
import pandas as pd
import requests
import logging
import sqlite3
import os
import uuid
from google.cloud import bigquery
from dotenv import load_dotenv
import datetime


# Ajoute le chemin vers le dossier parent de Utils
sys.path.append(os.path.abspath(os.path.join("..", "..")))

from Utils.helpers import error, info, debug


def getMovies(title : str ="Pokemon", OMDB_API_KEY = None) -> pd.DataFrame :
    """
    Retourne la liste de tous les films portant le nom passé en parametre

    Args:
        title(str): Le titre ou mot clé du film
        OMDB_API_KEY(str): Api key
    Returns:
        pd.DataFrame
    """

    if OMDB_API_KEY == None:
        load_dotenv(override=True)
        OMDB_API_KEY = os.getenv("OMDB_API_KEY", None)

    if OMDB_API_KEY == None : 
        info("OMDB_API_KEY has not been found")
        return pd.DataFrame([])
    
    params = {"apikey": OMDB_API_KEY, "s": title}
    url = "https://www.omdbapi.com"
    df = []
    try:
        page = 1
        while True:
            params["page"] = str(page)
            response = requests.get(url, params)

            if response.status_code == requests.codes.ok :
                data = response.json()
                page += 1
                df.extend(data["Search"])
                
                if int(data["totalResults"]) == len(df):
                    break
            else :
                error(f"Request failed with : {response.status_code} : {response.text}")
                break
    except Exception as e:
        error(f"Request failed with {e}")
        df = []

    df_movies = [movie for movie in df]
    return pd.DataFrame(df_movies)


def getAllPokemon() -> list:
    """
    Retourne la liste de tous les pokemons

    Returns:
        list
    """
    url = "https://pokeapi.co/api/v2/pokemon?limit=1000"
    df = []
    try:
        while True:
            debug(f"Appel à url = {url}")
            response = requests.get(url)

            if response.status_code == requests.codes.ok :
                data = response.json()
                url = data["next"]
                df.extend(data["results"])
                
                if int(data["count"]) == len(df):
                    break
    except Exception as e:
        error(f"Request failed with {e}")
        df = []
    return df



def sqlite_save(path = "pokemon.db", pokemons = []) -> int:
    """
    Ecriture du tableau de pokemon dans la table pokemon

    Args:
        path(str)
    
    Returns
        int
    """
    count = 0
    conn = None
    values = []
    try:
        # Db Creation if not exist
        create_db(path)

        conn = sqlite3.connect(path)
        cursor = conn.cursor()
        
        values = [(str(uuid.uuid4()), pokemon["name"], pokemon["url"]) for pokemon in pokemons]
        values = set(values)
        
        logging.info(values)
        cursor.executemany("""
                            INSERT INTO pokemon (id, name, url)
                            VALUES (?, ?, ?)
                            """, values)
        
        cursor.execute("SELECT COUNT(*) FROM pokemon")
        count = cursor.fetchone()[0]

    except Exception as e:
        error(f"DataBase save failed with values = {values} and error = {e}")
    finally:
        if conn:
            conn.commit()
            conn.close()

    return count


def create_db(path) -> bool:
    """
    Creation de la table 

    Args:
        path(str)
    
    Returns
        bool
    """
    try:
        conn = sqlite3.connect(path)
        cursor = conn.cursor()

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS pokemon (
            id TEXT PRIMARY KEY,
            name TEXT,
            url TEXT
        )
        """)
    except Exception as e:
        error(f"DB Creation failed with {e}")
        return False

    return True

def sanitize_row(row):
    return {
        k: (str(v) if isinstance(v, (pd.Timestamp, datetime.datetime)) else v)
        for k, v in row.items()
    }

def save_list_on_bigquery(data: pd.DataFrame, my_table: str, schema=None) -> bool:
    """
    Sauvegarde la liste en fichier json temporaire pour le charger via un job bigquery

    Args:
        data(pd.DataFrame): Tableau de donnée
        my_table(str): Big query table name
    
        Return :
            bool
    """
    temp_path = "temp.json"

    data.to_json(temp_path, orient="records", lines=True, force_ascii=False)

    client = bigquery.Client()
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect=(schema is None),
    )
    
    if schema:
        job_config.schema = schema
    
    with open(temp_path, "rb") as source_file:
        job = client.load_table_from_file(source_file, my_table, job_config=job_config)

    job.result()  # Attendre la fin du job
    
    # 4. Nettoyage du fichier temporaire
    os.remove(temp_path)
    return True




if __name__ == "main":
    info("Start process")

    # Extract All pokemon moovies
    df_moovies = getMovies()
    info("Extract All Movies")

    # Extract All pokemon
    pokemons = getAllPokemon()
    info("Extract All Pokemons")

    # Save moovies in CSV
    moovies_path = "../../00_assets/movies.csv"
    df_moovies.to_csv(moovies_path)
    info("Save movies in CSV")

    path = "pokemon_movies.db"
    sqlite_save(path, pokemons)
    info("Save pokemons in SQlite Data base")

    info("Process finished")