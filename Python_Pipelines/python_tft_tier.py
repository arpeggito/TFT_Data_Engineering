# from datetime import datetime
# from airflow.decorators import dag, task
import requests
import pandas as pd
import json
from sqlalchemy import create_engine
import time
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
DB_1_NAME = os.getenv("DB_1_NAME")
DB_2_NAME = os.getenv("DB_2_NAME")

headers = {"X-Riot-Token": RIOT_API_KEY}

BASE_URL = "https://euw1.api.riotgames.com/tft"
BASE_URL_2 = "https://europe.api.riotgames.com/tft"

summoner_id = []
summoner_name = []
league_points = []
wins = []
losses = []
player_puuid = []

match_summ_name = []
match_augments = []
match_placement = []
match_units = []
match_level = []
match_id = []


chall_leaderboard = {
    "Summoner Name": summoner_id,
    "League Points": league_points,
    "Number of Wins": wins,
    "Number of Losses": losses,
}

chall_matches = {
    "Summoner Name": match_summ_name,
    "Augments": match_augments,
    "Units": match_units,
    "Level": match_level,
    "Placement": match_placement,
    "Match ID": match_id,
}

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
def tft_challenger_rank():
    url = f"{BASE_URL}/league/v1/challenger?queue=RANKED_TFT"
    response = requests.get(url, headers=headers)
    response_json = json.loads(response.text)
    entries = response_json["entries"]

    for entry in entries:
        data_summonerId = entry["summonerId"]
        data_summonerName = entry["summonerName"]
        data_summonerLP = entry["leaguePoints"]
        data_summonerWin = entry["wins"]
        data_summonerLoss = entry["losses"]

        summonerId.append(data_summonerId)
        summonerName.append(data_summonerName)
        leaguePoints.append(data_summonerLP)
        wins.append(data_summonerWin)
        losses.append(data_summonerLoss)

    df = pd.DataFrame(chall_leaderboard)
    df.insert(4, "Rank", "Challenger", True)

    # # Connects to the Postgres DB to add the DF to the existent DB called tft
    engine = create_engine(
        f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_1_NAME}"
    )
    with engine.connect() as connection:
        df.to_sql(
            "tft_challenger_leaderboard", engine, if_exists="replace", index=False
        )
    
    conn = snowflake.connector.connect(
        user= os.getenv("SNOWFLAKE_USER"),
        password= os.getenv("SNOWFLAKE_PASSWORD"),
        account= os.getenv("SNOWFLAKE_ACCOUNT_ID"),
        warehouse= os.getenv("SNOWFLAKE_WAREHOUSE"),
        database= os.getenv("SNOWFLAKE_DATABASE"),
    )
    # ToDO: extract to config
    schema_name = "TFT_DATABASE"
    table_name = "tft_challenger_leaderboard"
    cur = conn.cursor()
    cur.execute("USE SCHEMA TFT")
    
    status, num_chunks, num_rows, output = write_pandas(
    conn,
    df,
    schema=schema_name,
    table_name=table_name,
    database=os.getenv("SNOWFLAKE_DATABASE"),
    auto_create_table=True,
    overwrite=True
    )
    
# @task
@sleep_and_retry
@limits(calls=rate_limit, period=time_period)
def tft_chall_matches():
    for name in summonerName:
        try:
            # Gets the puuid of each challenger player
            url_puuid = f"{BASE_URL}/summoner/v1/summoners/by-name/{name}"
            response = requests.get(url_puuid, headers=headers)
            response_json = json.loads(response.text)
            puuid = response_json["puuid"]

            # Gets the Matches by puuid of each player
            url_matches = (
                f"{BASE_URL_2}/match/v1/matches/by-puuid/{puuid}/ids?start=0&count=10"
            )
            response2 = requests.get(url_matches, headers=headers)
            matches = json.loads(response2.text)

            # Iterates over the matches and brings the important data from the match of the player
            for match in matches:
                url_matchid = f"{BASE_URL_2}/match/v1/matches/{match}"
                response3 = requests.get(url_matchid, headers=headers)
                game = json.loads(response3.text)
                data = game["info"]["participants"][game["metadata"]["participants"].index(puuid)]
                if data == -1:
                    break

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

        except KeyError:
            print(response.status_code)
        except Exception as e:
            print(e)

    # Converts the Dictionary with the data from the previous for loops into a DF
    df_chall_matches = pd.DataFrame(chall_matches)
    df_without_underscore = rm_underscore(df_chall_matches)
    
    # # Sends the dataframe to the postgres database.
    engine = create_engine(
        f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_2_NAME}"
    )
    with engine.connect() as connection:
        df_without_underscore.to_sql(
            "chall_tft_stats", engine, if_exists="replace", index=False
        )
        
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
    df_without_underscore,
    schema=schema_name,
    table_name=table_name,
    database=os.getenv("SNOWFLAKE_DATABASE"),
    auto_create_table=True,
    overwrite=True
    )
    
def main():
    try:
        tft_challenger_rank()
        tft_chall_matches()
    except RateLimitException:
        logging.error("Rate limit exceeded. Please wait before making more requests.")
    except Exception as e:
        logging.error(f"An error occurred: {e}")

if __name__ == "__main__":
    main()