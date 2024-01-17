from riotwatcher import TftWatcher
import pandas as pd
import numpy as np
import json
from sqlalchemy import create_engine
from datetime import datetime

def rm_underscore(df):
    # Apply a lambda function to remove underscores from each element in the DataFrame
    df_no_underscores = df.applymap(lambda x: str(x).replace('_', ''))

    return df_no_underscores

def tft_pipeline():
    api_key = 'RGAPI-e28b9518-7790-407b-99d8-61cc69943375'
    watcher = TftWatcher(api_key)
    my_region = 'euw1'
    summoner_name = 'Arpeggito'

    me = watcher.summoner.by_name(my_region, summoner_name)

    # for key in me:
    #     print(key, ':', me[key])

    matches_ids = watcher.match.by_puuid(my_region, me['puuid'], count=20)
    matches = [watcher.match.by_id(my_region, item) for item in matches_ids]

    match_augments = []
    match_placement = []
    match_units = []
    match_level = []
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

    DataFrame = {'augments': match_augments, 'placement': match_placement, 'units': match_units, 'level': match_level}
    # print(data)

    df = pd.DataFrame(DataFrame)
    df_without_underscore = rm_underscore(df)
    print(df)

    engine = create_engine('postgresql://postgres:pass123@192.168.0.134:5432/tft')
    df_without_underscore.to_sql('tft', engine, if_exists='replace', index=False)
