import os
import pyodbc
import azure.functions as func
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
import logging
import requests
import time

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('FetchPuuids function processing a request.')

    try:
        # Retrieve SQL credentials from Key Vault via Managed Identity
        vault_url = "https://myRiotDataKeyVault.vault.azure.net"
        credential = DefaultAzureCredential()
        secret_client = SecretClient(vault_url=vault_url, credential=credential)

        sql_user = secret_client.get_secret("FunctionAppSqlUser").value
        sql_password = secret_client.get_secret("FunctionAppSqlPassword").value

        # Retrieve server and database from environment variables
        sql_server = os.getenv("SQL_SERVER", "myriotdataserver.database.windows.net")
        sql_database = os.getenv("SQL_DATABASE", "myRiotDataSQL")

        # Build connection string
        connection_string = (
            f"Driver={{ODBC Driver 18 for SQL Server}};"
            f"Server=tcp:{sql_server},1433;"
            f"Database={sql_database};"
            f"Uid={sql_user};"
            f"Pwd={sql_password};"
            "Encrypt=yes;"
            "TrustServerCertificate=no;"
            "Connection Timeout=30;"
        )

        # Riot API Key
        riot_api_key = secret_client.get_secret("RiotApiKey").value
        if not riot_api_key:
            raise ValueError("RiotApiKey is not set in Key Vault!")

        headers = {"X-Riot-Token": riot_api_key}

        # Fetch summoners without PUUID
        with pyodbc.connect(connection_string, autocommit=True) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT SummonerID, Region FROM Summoners WHERE PUUID IS NULL")
            rows = cursor.fetchall()

            batch_size = 100
            all_batches_success = True
            all_batch_puuids = []

            for i in range(0, len(rows), batch_size):
                batch = rows[i:i + batch_size]
                batch_success = True
                batch_puuids = []

                for summoner_id, region in batch:
                    puuid_url = f"https://{region}.api.riotgames.com/lol/summoner/v4/summoners/{summoner_id}"
                    response = requests.get(puuid_url, headers=headers)

                    if response.status_code == 200:
                        puuid = response.json()["puuid"]
                        batch_puuids.append((puuid, summoner_id, region))
                    else:
                        batch_success = False
                        logging.error(f"Failed to fetch PUUID for SummonerID {summoner_id} in {region}: {response.text}")
                        break

                if batch_success:
                    all_batch_puuids.extend(batch_puuids)
                    logging.info(f"Successfully processed batch starting with SummonerID {batch[0][0]}")
                else:
                    all_batches_success = False
                    logging.error(f"Batch starting with SummonerID {batch[0][0]} failed. Aborting further processing.")
                    break

                # Respect Riot API rate limits
                logging.info("Waiting 2 minutes to respect Riot API rate limits...")
                time.sleep(120)

            if all_batches_success:
                for puuid, summoner_id, region in all_batch_puuids:
                    cursor.execute(
                        """
                        UPDATE Summoners
                        SET PUUID = ?
                        WHERE SummonerID = ? AND Region = ?
                        """,
                        puuid, summoner_id, region
                    )
                logging.info("All batches succeeded. Database successfully updated.")
                return func.HttpResponse("PUUIDs fetched and database updated successfully!", status_code=200)
            else:
                logging.error("Not all batches succeeded. No updates were made to the database.")
                return func.HttpResponse("Failed to process all batches. No updates were made.", status_code=500)

    except Exception as e:
        logging.error(f"Error in FetchPuuids: {e}")
        return func.HttpResponse(f"Error occurred: {str(e)}", status_code=500)