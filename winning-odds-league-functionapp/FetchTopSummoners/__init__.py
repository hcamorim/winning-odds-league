import os
import pyodbc
import azure.functions as func
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
import logging
import requests
import time

def connect_to_database(connection_string, retries=3, delay=5):
    for attempt in range(retries):
        try:
            logging.info(f"Attempting to connect to the database (Attempt {attempt + 1}/{retries})...")
            conn = pyodbc.connect(connection_string, autocommit=True)
            logging.info("Database connection established successfully.")
            return conn
        except pyodbc.Error as e:
            logging.warning(f"Database connection attempt {attempt + 1} failed: {e}")
            if attempt < retries - 1:
                logging.info(f"Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                logging.error("All retry attempts to connect to the database failed.")
                raise

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('FetchChallengerSummoners function processing a request.')

    try:
        # Retrieve SQL credentials from Key Vault
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
        headers = {"X-Riot-Token": riot_api_key}

        # Regions to fetch
        regions = ["euw1", "eun1", "kr", "na1"]
        summoners = []

        # Fetch Challenger summoners
        base_url = "https://{region}.api.riotgames.com/lol/league/v4/challengerleagues/by-queue/RANKED_SOLO_5x5"
        for region in regions:
            url = base_url.format(region=region)
            response = requests.get(url, headers=headers)

            if response.status_code == 429:  # Rate limit exceeded
                retry_after = int(response.headers.get("Retry-After", 60))
                logging.warning(f"Rate limit exceeded. Retrying after {retry_after} seconds...")
                time.sleep(retry_after)
                continue

            if response.status_code != 200:
                raise Exception(f"Failed to fetch Challenger summoners for {region}: {response.text}")

            challenger_data = response.json()
            if "entries" not in challenger_data:
                raise Exception(f"Unexpected API response format: {challenger_data}")

            for entry in challenger_data["entries"]:
                summoners.append({
                    "summonerID": entry["summonerId"],
                    "rank": "Challenger",
                    "region": region
                })

        # Ensure summoners are fetched
        if not summoners:
            logging.info("No summoners fetched from the API.")
            return func.HttpResponse("No summoners fetched from the API.", status_code=204)

        # Update SQL database
        with connect_to_database(connection_string) as conn:
            cursor = conn.cursor()

            create_table_sql = """
            IF NOT EXISTS (
                SELECT *
                FROM sys.tables
                WHERE name = 'Summoners'
            )
            BEGIN
                CREATE TABLE Summoners (
                    ID INT IDENTITY(1,1) PRIMARY KEY,
                    SummonerID VARCHAR(100) NOT NULL,
                    Rank VARCHAR(50) NOT NULL,
                    Region VARCHAR(10) NOT NULL,
                    PUUID VARCHAR(100) NULL
                );
            END
            """
            cursor.execute(create_table_sql)

            # Insert summoners
            for summoner in summoners:
                cursor.execute(
                    """
                    IF NOT EXISTS (
                        SELECT 1 FROM Summoners
                        WHERE SummonerID = ? AND Region = ?
                    )
                    BEGIN
                        INSERT INTO Summoners (SummonerID, Rank, Region)
                        VALUES (?, ?, ?)
                    END
                    """,
                    (summoner["summonerID"], summoner["region"],  # For the IF NOT EXISTS check
                     summoner["summonerID"], summoner["rank"], summoner["region"])  # For the INSERT
                )

        return func.HttpResponse("Successfully fetched and stored Challenger summoners!", status_code=200)

    except Exception as e:
        logging.error(f"Error in FetchChallengerSummoners: {e}")
        return func.HttpResponse(f"Error occurred: {str(e)}", status_code=500)