import requests
import pandas
import json
from riotwatcher import TftWatcher

url = 'https://euw1.api.riotgames.com/tft/league/v1/challenger?queue=RANKED_TFT&api_key=RGAPI-e6afb4d5-71c4-4b3a-8a3b-ec3b2af85334'



response = requests.get(
    url=url,
)

response_json = json.loads(response.text)

with open('response_json.json', 'w') as file:
    json.dump(response_json, file, indent=4)

print(response_json)