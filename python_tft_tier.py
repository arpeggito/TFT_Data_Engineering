# from datetime import datetime
# from airflow.decorators import dag, task
import requests
import pandas as pd
import json
from riotwatcher import TftWatcher
from sqlalchemy import create_engine
import time
import logging

API_KEY = 'RGAPI-4aca0822-6d0c-4f94-a5fb-9416a23151dd'
headers = {"X-Riot-Token": "RGAPI-4aca0822-6d0c-4f94-a5fb-9416a23151dd"}

summonerId = []
summonerName = []
leaguePoints = []
wins = []
losses = []
player_puuid = []

match_summ_name=[]
match_augments = []
match_placement = []
match_units = []
match_level = []

chall_leaderboard = {'Summoner Name': summonerName, 'League Points': leaguePoints, 'Number of Wins': wins, 'Number of Losses': losses}

chall_matches = {'Summoner Name': match_summ_name,'Augments': match_augments, 'Units': match_units, 'Level': match_level, 'Placement': match_placement}

# @dag(schedule="@daily", start_date=datetime(2024, 1, 22))
# def taskflow():
def rm_underscore(df):
    # Apply a lambda function to remove underscores from each element in the DataFrame
    df_no_underscores = df.applymap(lambda x: str(x).replace('_', ''))

    return df_no_underscores

# @task
def tft_challenger_rank():
    url = 'https://euw1.api.riotgames.com/tft/league/v1/challenger?queue=RANKED_TFT'
    response = requests.get(url, headers=headers)
    response_json = json.loads(response.text)
    entries = response_json['entries']

    for entry in entries:
        data_summonerId = entry['summonerId']
        data_summonerName = entry['summonerName']
        data_summonerLP = entry['leaguePoints']
        data_summonerWin = entry['wins']
        data_summonerLoss = entry['losses']
        
        summonerId.append(data_summonerId)
        summonerName.append(data_summonerName)
        leaguePoints.append(data_summonerLP)
        wins.append(data_summonerWin)
        losses.append(data_summonerLoss)

# @task
def tf_to_df():
    df = pd.DataFrame(chall_leaderboard)
    df.insert(4, "Rank", 'Challenger', True)
    # print(df)

    # Connects to the Postgres DB to add the DF to the existent DB called tft
    engine = create_engine('postgresql://postgres:pass123@192.168.0.134:5432/tft_challenger')
    df.to_sql('tft_challenger', engine, if_exists='replace', index=False)

# @task
def tft_chall_matches():
    for name in summonerName:
        try:
            # Gets the puuid of each challenger player
            url_puuid = f'https://euw1.api.riotgames.com/tft/summoner/v1/summoners/by-name/{name}'
            response = requests.get(url_puuid, headers=headers)
            response_json = json.loads(response.text)
            puuid = response_json['puuid']
            time.sleep(1.2)

            # Gets the Matches by puuid of each player
            url_matches = f'https://europe.api.riotgames.com/tft/match/v1/matches/by-puuid/{puuid}/ids?start=0&count=10'
            response2 = requests.get(url_matches, headers=headers)
            matches = json.loads(response2.text)
            time.sleep(1.2)

            # Iterates over the matches and brings the important data from the match of the player
            for match in matches:
                url_matchid = f'https://europe.api.riotgames.com/tft/match/v1/matches/{match}'
                response3 = requests.get(url_matchid, headers=headers)
                game = json.loads(response3.text)
                time.sleep(1.2)
                data = game['info']['participants'][game['metadata']['participants'].index(puuid)]
                if data == -1:
                    break

                # Manipulats the data and cleans it
                augments = data['augments']
                augments_new = [e[13:] for e in augments]
                placements = data['placement']
                units =  [unit['character_id'] for unit in data['units']]
                units_new = [e[6:] for e in units]
                level = data['level']

                # Append the data into a list to convert it into a DF
                match_summ_name.append(name)
                match_augments.append(augments_new)
                match_placement.append(placements)
                match_units.append(units_new)
                match_level.append(level)

        except KeyError:
            print(response.status_code)
        except Exception as e: 
            print(e)

# Converts the Dictionary with the data from the previous for loops into a DF
# @task
def chall_matches_to_df():
    df_chall_matches = pd.DataFrame(chall_matches)
    df_without_underscore = rm_underscore(df_chall_matches)

    # Sends the dataframe to the postgres database.
    engine = create_engine('postgresql://postgres:pass123@192.168.0.134:5432/chall_tft_stats')
    df_without_underscore.to_sql('chall_tft_stats', engine, if_exists='replace', index=False)

    # logging.info('Start data Pipeline')
    # tft_challenger_rank() >> tf_to_df() >> tft_chall_matches() >> chall_matches_to_df()

tft_challenger_rank()
tf_to_df() 
tft_chall_matches()
chall_matches_to_df()