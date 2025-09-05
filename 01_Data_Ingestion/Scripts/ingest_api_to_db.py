

import pandas as pd

from Utils.helpers import getMovies, getAllPokemon, info, sqlite_save

if __name__ == "main":
    info("Strat process")

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

    info("Fin du traitement")