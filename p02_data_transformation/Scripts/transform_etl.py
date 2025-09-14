import pandas as pd
import requests
import os, sys
from dotenv import load_dotenv
from google.cloud import bigquery
import copy



from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

sys.path.append(os.path.abspath(os.path.join("..", "..")))

from Utils.helpers import error, info, generate_bq_schema_from_df

from p01_data_ingestion.scripts.ingest_api_to_db import save_list_on_bigquery

def transform_pokemon(spark: SparkSession, csv_file="movies.csv", text_file="pokemon.txt") -> tuple:
    """
        Extract Movies and pokemon list and get More information to enrich the data frame

        Args:
            spark(SparkSession): SparkSession
            csv_file(str): Csv file path
            text_file(str): text file path

        Return:
            tuple
    """
    
    csv_uri_base = "gs://de-01-data-ingestion/csv/"
    text_uri_base = "gs://de-01-data-ingestion/text/"

    schema = StructType([
        StructField("ID", StringType(), True),
        StructField("Title", StringType(), True),
        StructField("Year", StringType(), True),
        StructField("imdbID", StringType(), True),
        StructField("Type", StringType(), True),
        StructField("Poster", StringType(), True),
    ])

    csv_df = spark.createDataFrame([], schema)    
    json_data = pd.array([])  

    try:
        csv_df = spark.read.schema(schema).option("header", True).csv(csv_uri_base + csv_file, schema)
    except Exception as e:
        error(f"Csv loading with error {e}")

    try:
        json_data = pd.read_json(text_uri_base + text_file)
    except Exception as e:
        error(f"Json loading with error {e}")

    movies_tiles = set((row["Title"], row["imdbID"]) for row in csv_df.select("Title", "imdbID").collect())

    pokemon_names = json_data.loc[:,"name"] # type: ignore
    pokemon_names = set(pokemon_names)

    info(json_data.shape[0]) # type: ignore

    pokemons_movies = []
    pokemons = []

    for title in movies_tiles:

        try:
            pokemon_movie = get_pokemon(title[1])

            words = title[0].split()
            for word in words:
                if word.lower() in pokemon_names:
                    
                    pokemon_row = json_data[json_data["name"]==word.lower()] # type: ignore

                    url = pokemon_row["url"].values[0]

                    if url.split("/")[-2] not in [pok["id"] for pok in pokemons]:
                        response = requests.get(url)

                        if response.status_code == requests.codes.ok :
                            pokemon = response.json()
                            pokemon.pop("moves", None) # type: ignore
                            pokemon.pop("sprites", None) # type: ignore

                            pokemon_movie["pokemon_id"] = pokemon["id"] # type: ignore

                            pokemons.append(pokemon)
                    break
            pokemons_movies.append(pokemon_movie)
        except Exception as e:
            error(f"Request failed with {e}")

    pokemon_movie_df = pd.DataFrame(pokemons_movies)

    pokemons_df = pd.DataFrame(pokemons)

    return (pokemon_movie_df, pokemons_df, pokemons_movies, pokemons)


def get_pokemon(imdbId: str, OMDB_API_KEY = None) -> dict | None:
    """
    Get All pokemon movie information
    
    Args:
        imdbId(str): L'id du film
        OMDB_API_KEY(str): Api key

    Returns:
        dict
    """

    if OMDB_API_KEY == None:
        load_dotenv(override=True)
        OMDB_API_KEY = os.getenv("OMDB_API_KEY", None)

    if OMDB_API_KEY == None : 
        info("OMDB_API_KEY has not been found")
        return None
    
    params = {"apikey": OMDB_API_KEY, "i": imdbId}
    url = "https://www.omdbapi.com"
    data = {}
    try:
        response = requests.get(url, params)

        if response.status_code == requests.codes.ok :
            data = response.json()
        else :
            error(f"Request failed with : {response.status_code} : {response.text}")
    except Exception as e:
        error(f"Pokemon movie request failed with {e}")

    return data


def get_spark_config():
    """
        Load Env variables and config spark session
        
        Return :
            SparkSession
    """
    load_dotenv(override=True)
    creds_json = os.getenv("GCP_CREDENTIALS_SECRET")

    assert creds_json is not None
    assert os.path.exists(creds_json)

    gcs_jar_path = os.path.abspath("..\\libs\\gcs-connector-hadoop3-2.2.2-shaded.jar")
    assert os.path.exists(gcs_jar_path), f"Fichier JAR introuvable : {gcs_jar_path}"


    PYSPARK_PYTHON = os.getenv("PYSPARK_PYTHON")
    PYSPARK_DRIVER_PYTHON = os.getenv("PYSPARK_DRIVER_PYTHON")
    HADOOP_HOME = os.getenv("HADOODP_HOME")

    assert PYSPARK_PYTHON is not None
    assert PYSPARK_DRIVER_PYTHON is not None
    assert HADOOP_HOME is not None

    os.environ["PYSPARK_PYTHON"] = PYSPARK_PYTHON
    os.environ["PYSPARK_DRIVER_PYTHON"] = PYSPARK_DRIVER_PYTHON
    os.environ["HADOOP_HOME"] = HADOOP_HOME
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = creds_json

    return SparkSession.builder \
    .appName("GCS Loader") \
    .config("spark.hadoop.hadoop.native.lib", "false")\
    .config("spark.jars", gcs_jar_path)\
    .master("local[*]")\
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .config("spark.hadoop.google.cloud.auth.null.enable", "true") \
    .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", creds_json) \
    .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")\
    .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")\
    .getOrCreate()


def change_rating_value(ratings):
    if not isinstance(ratings, list):
        return ratings
    
    if len(ratings) == 0 :
        return None
    
    rate = 0
    for rating in ratings:
        values = rating["Value"].split("/")
        if not "%" in values[0] :
            rate += float(values[0])
        else:
            values =  values[0].split("%") 
            rate += float(values[0])/10

    return  rate

def change_abilities_is_hidden_value(abilities):
    for index, value in enumerate(abilities):
        value["is_hidden"] = bool(value["is_hidden"])

        abilities[index] = value
    
    return abilities

def change_past_abilities_is_hidden_value(past_abilities):
    info(f"past_abilities = {past_abilities}")
    if not isinstance(past_abilities, list):
        return None
    
    for index, value in enumerate(past_abilities):
        info(f"past_abilities {index} = {value}")

        abilities = value["abilities"]
        for sub_index, sub_value in enumerate(abilities):
            sub_value["is_hidden"] = bool(sub_value["is_hidden"])
            abilities[sub_index] = sub_value
        
        value["abilities"] = abilities
        past_abilities[index] = value
    
    info(f"past_abilities = {past_abilities}")
    return past_abilities
    

def change_date_value(rating):
    if not isinstance(rating, dict):
        return rating
    
    values = rating["Value"].split("/")
    return values[0]

def change_year_value(year):
    """
    Changer la valeur de Year parfois xxxx-xxxx ou xxxx- en xxxx
    """
    if not isinstance(year, str):
        return year
    
    if not any(t in year for t in ["-", "–", "—"]):
        return year
    value = year[:4]
    return value

def pokemon_movies_cleaning(movies_list: pd.DataFrame) -> pd.DataFrame:

    df = copy.deepcopy(movies_list)

    df = df[df["Released"] != "N/A"]
    

    df = df.fillna("None")
    df.replace(["NaN", "N/A", "None", "nan"], None, inplace=True)
    df = df.where(pd.notnull(df), None)
    
    df["Ratings"] = df["Ratings"].apply(change_rating_value)
    df["Ratings"] = df["Ratings"].astype("float64")

    df["Released"] = pd.to_datetime(df["Released"], format="%d %b %Y", errors="coerce")
    df["Released"] = df["Released"].astype(str)

    df["Year"] = df["Year"].astype(str)
    df["Year"] = df["Year"].apply(change_year_value)

    df["pokemon_id"] = df["pokemon_id"].astype("Int64")

    return df



def pokemons_cleaning(pokemon_list: pd.DataFrame) -> pd.DataFrame:
    
    df = copy.deepcopy(pokemon_list)
    df = df.fillna("None")
    df.replace(["NaN", "N/A", "None", "nan"], None, inplace=True)
    df = df.where(pd.notnull(df), None)

    
    df["abilities"] = df["abilities"].apply(change_abilities_is_hidden_value)
    df["past_abilities"] = df["past_abilities"].apply(change_past_abilities_is_hidden_value)
    
    return df


def get_pokemon_movies_schema(df: pd.DataFrame) :
    """
    Généré via le dataframe et retourné le schema pour les films pokemons

    Args:
        df(pd.DataFrame): Le dataframe
    
    Returns:
        list
    """
    my_schema = [
        bigquery.SchemaField("Year", "INTEGER"),
        bigquery.SchemaField("Ratings", "FLOAT"),
        bigquery.SchemaField("imdbRating", "FLOAT"),
        bigquery.SchemaField("imdbVotes", "FLOAT"),
        bigquery.SchemaField("imdbID", "STRING", description="Table unique key", mode="REQUIRED"),
        bigquery.SchemaField("totalSeasons", "INTEGER"),
        bigquery.SchemaField("pokemon_id", "INTEGER", description="pokemon table foreign key"),
    ]
    return generate_bq_schema_from_df(df, my_schema)

def get_pokemons_schema(df: pd.DataFrame) :
    """
    Généré via le dataframe et retourné le schema de la liste des pokemons

    Args:
        df(pd.DataFrame): Le dataframe
    
    Returns:
        list
    """
    my_schema = [
        bigquery.SchemaField("id", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("order", "INTEGER"),
        bigquery.SchemaField("weight", "INTEGER"),
        bigquery.SchemaField("is_default", "BOOLEAN"),
        bigquery.SchemaField("base_experience", "INTEGER"),
        bigquery.SchemaField("height", "FLOAT"),
        bigquery.SchemaField("game_indices", "RECORD", mode="REPEATED", fields=[
            bigquery.SchemaField("game_index", "INTEGER"),
            bigquery.SchemaField("version","RECORD", mode="NULLABLE", fields=[
                    bigquery.SchemaField("name", "STRING"),
                    bigquery.SchemaField("url", "STRING"),
                ],
            ),
        ]),
        bigquery.SchemaField("abilities", "RECORD", mode="REPEATED", fields=[
            bigquery.SchemaField("is_hidden", "BOOLEAN", mode="NULLABLE"),
            bigquery.SchemaField("slot", "INTEGER"),
            bigquery.SchemaField("ability", "RECORD", fields=[
                    bigquery.SchemaField("name", "STRING"),
                    bigquery.SchemaField("url", "STRING"),
            ]),
        ]),
        bigquery.SchemaField("past_abilities", "RECORD", mode="REPEATED", fields=[
            bigquery.SchemaField("abilities", "RECORD", mode="REPEATED", fields=[
                bigquery.SchemaField("ability", "STRING"),
                bigquery.SchemaField("slot", "INTEGER"),
                bigquery.SchemaField("is_hidden", "BOOLEAN", mode="NULLABLE"),
            ]),
            bigquery.SchemaField("generation", "RECORD", mode="REPEATED", fields=[
                    bigquery.SchemaField("name", "STRING"),
                    bigquery.SchemaField("url", "STRING"),
            ]),
        ]),
        bigquery.SchemaField("stats", "RECORD", mode="REPEATED", fields=[
            bigquery.SchemaField("base_stat", "INTEGER"),
            bigquery.SchemaField("effort", "INTEGER"),
            bigquery.SchemaField("stat", "RECORD", mode="REPEATED", fields=[
                    bigquery.SchemaField("name", "STRING"),
                    bigquery.SchemaField("url", "STRING"),
            ]),
        ]),
        bigquery.SchemaField("types", "RECORD", mode="REPEATED", fields=[
                bigquery.SchemaField("slot", "INTEGER"),
                bigquery.SchemaField("type", "RECORD", mode="REPEATED", fields=[
                    bigquery.SchemaField("name", "STRING"),
                    bigquery.SchemaField("url", "STRING"),
                ]),
        ]),
        bigquery.SchemaField("past_types", "RECORD", mode="REPEATED", fields=[
            bigquery.SchemaField(
                "generation", "RECORD", mode="NULLABLE", fields=[
                    bigquery.SchemaField("name", "STRING"),
                    bigquery.SchemaField("url", "STRING"),
                ]
            ),
            bigquery.SchemaField(
                    "types", "RECORD", mode="REPEATED", fields=[
                        bigquery.SchemaField("slot", "INTEGER"),
                        bigquery.SchemaField(
                            "type", "RECORD", mode="NULLABLE", fields=[
                                bigquery.SchemaField("name", "STRING"),
                                bigquery.SchemaField("url", "STRING"),
                            ]
                        ),
                    ]
                )
            ])
        ]
    return generate_bq_schema_from_df(df, my_schema)


if __name__ == "main":
    info("Start process")
    spark = get_spark_config()

    datas = transform_pokemon(spark)
    info(f"Transform Data ({len(datas[0])}, {len(datas[1])})")

    #Save movies on BigQuery
    pokemon_movies_schema = get_pokemon_movies_schema(datas[0])

    pokemons_movies = pokemon_movies_cleaning(datas[0])
    save_list_on_bigquery(data=pokemons_movies, my_table="dataengineer-471201.pokemon_ds.pokemon_movies", schema=pokemon_movies_schema)
    info("Save pokemons movies")

    #Save pokemon list on BigQuery
    pokemon_schema = get_pokemons_schema(datas[1])

    pokemons = pokemons_cleaning(datas[1])
    save_list_on_bigquery(data=pokemons , my_table="dataengineer-471201.pokemon_ds.pokemons", schema=pokemon_schema)
    info("Process finished")


