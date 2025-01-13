import os
import requests
from dotenv import load_dotenv
import json
import time
from itertools import islice

# Load environment variables from .env file
load_dotenv()
API_KEY = os.getenv('RIOT_API_KEY')

# Riot API endpoints
REGION = 'euw1'
QUEUE = 'RANKED_SOLO_5x5'
CHALLENGER_URL = f"https://{REGION}.api.riotgames.com/lol/league/v4/challengerleagues/by-queue/{QUEUE}"
SUMMONER_URL_TEMPLATE = f"https://{REGION}.api.riotgames.com/lol/summoner/v4/summoners/{{summonerId}}"

# Headers with API key
headers = {
    "X-Riot-Token": API_KEY
}

def get_challenger_players():
    response = requests.get(CHALLENGER_URL, headers=headers)
    if response.status_code == 200:
        data = response.json()
        entries = data.get('entries', [])
        print(f"Total Challenger Players: {len(entries)}\n")
        summoner_ids = [player['summonerId'] for player in entries]
        # Save summoner IDs to a file
        with open('challenger_summoner_ids.json', 'w') as f:
            json.dump(summoner_ids, f, indent=4)
        print("Summoner IDs saved to challenger_summoner_ids.json")
    else:
        print(f"Failed to fetch Challenger data: {response.status_code} - {response.text}")

def get_account_info():
    # Load summoner IDs from the file
    try:
        with open('challenger_summoner_ids.json', 'r') as f:
            summoner_ids = json.load(f)
    except FileNotFoundError:
        print("summoner_ids file not found. Please run get_challenger_players() first.")
        return

    account_info = []
    batch_size = 100
    delay_seconds = 120  # 2 minutes

    # Helper generator to create batches
    def batched(iterable, n):
        it = iter(iterable)
        while True:
            batch = list(islice(it, n))
            if not batch:
                break
            yield batch

    total_batches = (len(summoner_ids) + batch_size - 1) // batch_size
    for batch_num, batch in enumerate(batched(summoner_ids, batch_size), start=1):
        print(f"Processing batch {batch_num} of {total_batches} ({len(batch)} summoner IDs)...")
        for summoner_id in batch:
            url = SUMMONER_URL_TEMPLATE.format(summonerId=summoner_id)
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                summoner_data = response.json()
                account_info.append(summoner_data)
            else:
                print(f"Failed to fetch account info for {summoner_id}: {response.status_code} - {response.text}")
        
        # Save current batch's account info
        with open(f'challenger_account_info_batch_{batch_num}.json', 'w') as f:
            json.dump(account_info, f, indent=4)
        print(f"Batch {batch_num} completed and saved to challenger_account_info_batch_{batch_num}.json")

        if batch_num < total_batches:
            print(f"Processed {batch_num * batch_size} calls. Waiting for 2 minutes to respect rate limits...\n")
            time.sleep(delay_seconds)
    
    print("All batches processed successfully.")

if __name__ == "__main__":
    choice = input("Choose an action:\n1. Fetch Challenger Summoner IDs\n2. Fetch Account Info\nEnter 1 or 2: ")
    if choice == '1':
        get_challenger_players()
    elif choice == '2':
        get_account_info()
    else:
        print("Invalid choice. Please enter 1 or 2.")