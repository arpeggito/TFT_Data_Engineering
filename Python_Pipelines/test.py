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

BASE_URL = "https://na1.api.riotgames.com/tft"
BASE_URL_2 = "https://americas.api.riotgames.com/tft"

# Define the rate limits
rate_limit = 20  # Number of requests per time period
time_period = 1  # Time period in seconds

def rm_underscore(df):
    # Apply a lambda function to remove underscores from each element in the DataFrame
    df_no_underscores = df.applymap(lambda x: str(x).replace("_", ""))

    return df_no_underscores

@sleep_and_retry
@limits(calls=rate_limit, period=time_period)
def tft_api_get_challenger():
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

def tft_api_transform_challenger():
    ## Try - Except
    raw_data = tft_api_get_challenger()
    
    summoner_id = []
    summoner_name = []
    league_points = []
    wins = []
    losses = []
    player_puuid = []

    ## for entries in raw_data 
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

    df = pd.DataFrame(chall_leaderboard)
    df.insert(4, "Rank", "Challenger", True)
    
    return df, summoner_name
    
# @task
@sleep_and_retry
@limits(calls=rate_limit, period=time_period)
def get_player_puuid(summoner_name) -> list[dict]:
    
    puuids = []
    
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
            puuids.append(data)
            
        except requests.HTTPError as http_err:
            print(f"HTTP error occurred: {http_err}")
            raise
        except Exception as err:
            print(f"Other error occurred: {err}")
            raise
        
    return puuids
        
@sleep_and_retry
@limits(calls=rate_limit, period=time_period)
def get_player_matches(puuids) -> list[dict]:
    
    matches_ids = []
    
    for user_data in puuids:
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
            
            matches_ids.append(user_data)
        
        except requests.HTTPError as http_err:
            print(f"HTTP error occurred: {http_err}")
            raise
        except Exception as err:
            print(f"Other error occurred: {err}")
            raise

    return matches_ids

@sleep_and_retry
@limits(calls=rate_limit, period=time_period)
def iterate_over_matches(matches_ids):
    try:
        # Iterates over the matches and brings the important data from the match of the player
        for user_data in matches_ids:
            
            matches = user_data["matches"]
            puuid = user_data["puuid"]
            
            for match in matches:
                url = f"{BASE_URL_2}/match/v1/matches/{match}"
                response = requests.get(url, headers=headers)
                response.raise_for_status()  # Raise an exception for HTTP errors

                game = response.json()
                
                data = game["info"]["participants"][game["metadata"]["participants"].index(puuid)]
                if data == -1:
                    break
            return data
    
    except requests.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
        raise
    except Exception as err:
        print(f"Other error occurred: {err}")
        raise
    
def matches_data_manipulation(data):
    
    match_summ_name = []
    match_augments = []
    match_placement = []
    match_units = []
    match_level = []
    match_id = []
    
    for match in data:
        # Manipulats the data and cleans it
        augments = data["augments"]
        augments_new = [e[13:] for e in augments]
        placements = data["placement"]
        units = [unit["character_id"] for unit in data["units"]]
        units_new = [e[6:] for e in units]
        level = data["level"]
        
        # Append the data into a list to convert it into a DF
        match_summ_name.append(name)
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
    
    return df_without_underscore
tft_api_get_challenger()
tft_api_transform_challenger()
df, summoner_name = tft_api_transform_challenger()
puuids = get_player_puuid(summoner_name)
matches_ids = get_player_matches(puuids)
data = iterate_over_matches(matches_ids)