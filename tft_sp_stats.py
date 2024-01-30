from riotwatcher import TftWatcher
import pandas as pd
import plotly.graph_objects as go
import numpy as np
import json
from sqlalchemy import create_engine
from datetime import datetime

def rm_underscore(df):
    # Apply a lambda function to remove underscores from each element in the DataFrame
    df_no_underscores = df.applymap(lambda x: str(x).replace('_', ''))

    return df_no_underscores

def tft_pipeline():
    # Definition of the API Authentication with Token
    api_key = 'RGAPI-4aca0822-6d0c-4f94-a5fb-9416a23151dd'
    watcher = TftWatcher(api_key)
    my_region = 'euw1'
    summoner_name = 'Arpeggito'
    
    # Retrieve information such as puuid, etc from a particular user from a particular region
    me = watcher.summoner.by_name(my_region, summoner_name)

    # for key in me:
    #     print(key, ':', me[key])

    # Obtain the last 20 matches ID's from your puuid and region.
    matches_ids = watcher.match.by_puuid(my_region, me['puuid'], count=20)
    # terate over those matches ID to get detail of each match
    matches = [watcher.match.by_id(my_region, item) for item in matches_ids]

    #Empty list to append different type of information from the different matches.
    match_augments = []
    match_placement = []
    match_units = []
    match_level = []

    # Iterates over each match to get the info of them and append them to the empty list, also performs data manipulation to modify the name of the information.
    for match in matches:
        data = match['info']['participants'][match['metadata']['participants'].index(me['puuid'])]
        augments = data['augments']
        augments_new = [e[13:] for e in augments]
        placements = data['placement']
        units =  [unit['character_id'] for unit in data['units']]
        units_new = [e[6:] for e in units]
        level = data['level']
        # print(units)
        
        match_augments.append(augments_new)
        match_placement.append(placements)
        match_units.append(units_new)
        match_level.append(level)

    # Converts the lists with the data into a dictionary
    DataFrame = {'augments': match_augments, 'placement': match_placement, 'units': match_units, 'level': match_level}
    # print(data) // Debug line

    # Converts the DataFrame Dict into a DataFrame
    # df_test = pd.json_normalize(DataFrame) /// test 
    
    df = pd.DataFrame(DataFrame)
    # df['placement'] = df['placement'].astype(int)
    # df['level'] = df['level'].astype(int)
    df_without_underscore = rm_underscore(df)

    print(df.dtypes)
    # print(df_without_underscore)

    # Connects to the Postgres DB to add the DF to the existent DB called tft
    engine = create_engine('postgresql://postgres:pass123@192.168.0.134:5432/arpeggito_stats')
    df_without_underscore.to_sql('arpeggito_stats', engine, if_exists='replace', index=False)

tft_pipeline()
