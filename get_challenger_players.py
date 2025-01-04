import os
import requests
from dotenv import load_dotenv
import json

# Load environment variables from .env file
load_dotenv()
API_KEY = os.getenv('RIOT_API_KEY')

# Riot API endpoint for Challenger League (Solo Queue) in EUW
REGION = 'euw1'
QUEUE = 'RANKED_SOLO_5x5'
URL = f"https://{REGION}.api.riotgames.com/lol/league/v4/challengerleagues/by-queue/{QUEUE}"

# Headers with API key
headers = {
    "X-Riot-Token": API_KEY
}

def get_challenger_players():
    response = requests.get(URL, headers=headers)
    if response.status_code == 200:
        data = response.json()
        print(json.dumps(data, indent=4))  # Pretty-print the JSON data
    else:
        print(f"Failed to fetch data: {response.status_code} - {response.text}")

if __name__ == "__main__":
    get_challenger_players()