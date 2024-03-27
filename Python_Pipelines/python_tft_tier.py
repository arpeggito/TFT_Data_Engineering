# from datetime import datetime
# from airflow.decorators import dag, task
import requests
import pandas as pd
import json
from sqlalchemy import create_engine
import logging
import os
from dotenv import load_dotenv
from ratelimit import RateLimitException, limits, sleep_and_retry
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

load_dotenv()

RIOT_API_KEY = os.getenv("RIOT_API_KEY")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")

DB_1_NAME = "tft_challenger_leaderboard"
DB_2_NAME = "chall_tft_stats"
DB_3_NAME =  "arpeggito_stats"

headers = {"X-Riot-Token": RIOT_API_KEY}

BASE_URL = "https://euw1.api.riotgames.com/tft"
BASE_URL_2 = "https://europe.api.riotgames.com/tft"


# Define the rate limits
rate_limit = 20  # Number of requests per time period
time_period = 1  # Time period in seconds


# @dag(schedule="@daily", start_date=datetime(2024, 1, 22))
# def taskflow():
def rm_underscore(df):
    # Apply a lambda function to remove underscores from each element in the DataFrame
    df_no_underscores = df.applymap(lambda x: str(x).replace("_", ""))

    return df_no_underscores

# @task
@sleep_and_retry
@limits(calls=rate_limit, period=time_period)
def tft_api_get_challenger():
    # API call to the challenger rank endpoint of the EU server
    try:
        url = f"{BASE_URL}/league/v1/challenger?queue=RANKED_TFT"
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raise an exception for HTTP errors
        
        response_json = response.json()
        
        return response_json
    except requests.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
        raise
    except Exception as err:
        print(f"Other error occurred: {err}")
        raise

def tft_api_transform_challenger() -> object:
    #Data Manipulation for the Challenger API call
    raw_data = tft_api_get_challenger()
    
    summoner_id = []
    summoner_name = []
    league_points = []
    wins = []
    losses = []
    player_puuid = []

    # Iterate over the players
    entries = raw_data["entries"]
    
    for entry in entries:
        data_summonerid = entry["summonerId"]
        data_summonername = entry["summonerName"]
        data_summonerLP = entry["leaguePoints"]
        data_summonerwin = entry["wins"]
        data_summonerloss = entry["losses"]

        summoner_id.append(data_summonerid)
        summoner_name.append(data_summonername)
        league_points.append(data_summonerLP)
        wins.append(data_summonerwin)
        losses.append(data_summonerloss)
    
    chall_leaderboard = {
    "Summoner Name": summoner_id,
    "League Points": league_points,
    "Number of Wins": wins,
    "Number of Losses": losses,
    }

    # Creates a Data Frame of the Leaderboard.
    df = pd.DataFrame(chall_leaderboard)
    # Insert a Column with the rank
    df.insert(4, "Rank", "Challenger", True)
    
    return df, summoner_name
    
# @task
@sleep_and_retry
@limits(calls=rate_limit, period=time_period)
def get_player_puuid(summoner_name) -> list[dict]:
    
    player_data = []
    
    for name in summoner_name:
        try:
            # Gets the puuid of each challenger player
            url_puuid = f"{BASE_URL}/summoner/v1/summoners/by-name/{name}"
            response = requests.get(url_puuid, headers=headers)
            response.raise_for_status()  # Raise an exception for HTTP errors
            
            response_json = response.json()      
            player_puuid = response_json["puuid"]
            
            data = {}
            data['name'] = name
            data['puuid'] = player_puuid
            player_data.append(data)
            
        except requests.HTTPError as http_err:
            print(f"HTTP error occurred: {http_err}")
            raise
        except Exception as err:
            print(f"Other error occurred: {err}")
            raise
        
    return player_data
        
@sleep_and_retry
@limits(calls=rate_limit, period=time_period)
def get_player_matches(player_data) -> list[dict]:
    
    player_data_matches_id = []
    
    for user_data in player_data:
        try:
            # Gets the Matches by puuid of each player
            puuid = user_data["puuid"]
            url_matches = (
                f"{BASE_URL_2}/match/v1/matches/by-puuid/{puuid}/ids?start=0&count=10"
            )
            response = requests.get(url_matches, headers=headers)
            response.raise_for_status()  # Raise an exception for HTTP errors
            matches = response.json()
            
            user_data["matches"] = matches
            
            player_data_matches_id.append(user_data)
        
        except requests.HTTPError as http_err:
            print(f"HTTP error occurred: {http_err}")
            raise
        except Exception as err:
            print(f"Other error occurred: {err}")
            raise

    return player_data_matches_id

@sleep_and_retry
@limits(calls=rate_limit, period=time_period)
def iterate_over_matches(player_data_matches_id) -> list[dict]:
    
    player_data_matches_detail = []
    
    try:
        # Iterates over the matches and brings the important data from the match of the player
        for user_data in player_data_matches_id:
            
            matches = user_data["matches"]
            puuid = user_data["puuid"]
            
            for match in matches:
                url = f"{BASE_URL_2}/match/v1/matches/{match}"
                response = requests.get(url, headers=headers)
                response.raise_for_status()  # Raise an exception for HTTP errors
                game = response.json()
                
                data = game["info"]["participants"][game["metadata"]["participants"].index(puuid)]
                
                user_data["Augments"] = data["augments"]
                user_data["Placement"] = data["placement"]
                user_data["Units"] = data["units"]
                user_data["Level"] = data["level"]
                user_data["Match"] = match
                
                player_data_matches_detail.append(user_data)

                if data == -1:
                    continue
                
        return player_data_matches_detail
    
    except requests.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
        raise
    except Exception as err:
        print(f"Other error occurred: {err}")
        raise
    
def matches_data_manipulation(player_data_matches_detail) -> object:
    
    match_summ_name = []
    match_augments = []
    match_placement = []
    match_units = []
    match_level = []
    match_id = []
    
    for data in player_data_matches_detail:
        print(data)
        # Manipulats the data and cleans it
        summoner_name = data["name"]
        match = data["Match"]
        augments = data["Augments"]
        augments_new = [e[13:] for e in augments]
        placements = data["Placement"]
        units = [unit["character_id"] for unit in data["Units"]]
        units_new = [e[6:] for e in units]
        level = data["Level"]
        
        # Append the data into a list to convert it into a DF
        match_summ_name.append(summoner_name)
        match_augments.append(augments_new)
        match_placement.append(placements)
        match_units.append(units_new)
        match_level.append(level)
        match_id.append(match)

    chall_matches = {
    "Summoner Name": match_summ_name,
    "Augments": match_augments,
    "Units": match_units,
    "Level": match_level,
    "Placement": match_placement,
    "Match ID": match_id,
}

    df_chall_matches = pd.DataFrame(chall_matches)
    df_without_underscore = rm_underscore(df_chall_matches)
    
    print(df_without_underscore.head())
    return df_without_underscore
    
def matches_to_sql(DB, dataframe) -> None:
    # Sends the dataframe to the postgres database.
    engine = create_engine(
        f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB}"
    )
    with engine.connect() as connection:
        dataframe.to_sql(
            DB, engine, if_exists="replace", index=False
        )
def matches_to_snowflake(dataframe) -> None:
    # Sends DataFrames to Snowflake
    conn = snowflake.connector.connect(
        user = os.getenv("SNOWFLAKE_USER"),
        password= os.getenv("SNOWFLAKE_PASSWORD"),
        account= os.getenv("SNOWFLAKE_ACCOUNT_ID"),
        warehouse= os.getenv("SNOWFLAKE_WAREHOUSE"),
        database= os.getenv("SNOWFLAKE_DATABASE"),
        role= os.getenv("SNOWFLAKE_ROLE")
    )
    
    # ToDO: extract to config

    schema_name = "TFT_DATABASE"
    table_name = "chall_tft_stats"
    cur = conn.cursor()
    cur.execute("USE SCHEMA TFT_DATABASE")
    
    status, num_chunks, num_rows, output = write_pandas(
    conn,
    dataframe,
    schema=schema_name,
    table_name=table_name,
    database=os.getenv("SNOWFLAKE_DATABASE"),
    auto_create_table=True,
    overwrite=True
    )
    
def main():
    try:
        tft_api_get_challenger()
        tft_api_transform_challenger()
        df, summoner_name = tft_api_transform_challenger()
        player_data = get_player_puuid(summoner_name)
        player_data_matches_id = get_player_matches(player_data)
        player_data_matches_detail = iterate_over_matches(player_data_matches_id)
        df_without_underscore = matches_data_manipulation(player_data_matches_detail)
        matches_to_sql(DB_2_NAME, df_without_underscore)
        matches_to_snowflake(df_without_underscore)

    except RateLimitException:
        logging.error("Rate limit exceeded. Please wait before making more requests.")
    except Exception as e:
        logging.error(f"An error occurred: {e}")

if __name__ == "__main__":
    main()