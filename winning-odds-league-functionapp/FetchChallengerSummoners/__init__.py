import os
import pyodbc
import azure.functions as func
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
import logging
import requests

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('FetchChallengerSummoners function processing a request.')

    try:
        # Retrieve SQL credentials from Key Vault via Managed Identity
        vault_url = "https://myRiotDataKeyVault.vault.azure.net"
        credential = DefaultAzureCredential()
        secret_client = SecretClient(vault_url=vault_url, credential=credential)

        sql_user = secret_client.get_secret("SQLUser").value
        sql_password = secret_client.get_secret("SQLPassword").value

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
            raise ValueError("RIOT_API_KEY environment variable not set!")

        headers = {"X-Riot-Token": riot_api_key}

        # Regions for Challenger summoners
        regions = ["euw1", "eun1", "kr", "na1"]
        summoners = []

        # Fetch Challenger summoners
        base_url = "https://{region}.api.riotgames.com/lol/league/v4/challengerleagues/by-queue/RANKED_SOLO_5x5"
        for region in regions:
            url = base_url.format(region=region)
            response = requests.get(url, headers=headers)

            if response.status_code != 200:
                raise Exception(f"Failed to fetch Challenger summoners for {region}: {response.text}")

            challenger_data = response.json()
            for entry in challenger_data["entries"]:
                summoners.append({
                    "summonerID": entry["summonerId"],
                    "rank": "Challenger",
                    "region": region
                })

        # Insert summoners into SQL database
        with pyodbc.connect(connection_string, autocommit=True) as conn:
            cursor = conn.cursor()

            # Ensure Summoners table exists
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

            # Insert summoners into the table
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
                    summoner["summonerID"], summoner["region"],
                    summoner["summonerID"], summoner["rank"], summoner["region"]
                )

        return func.HttpResponse("Successfully fetched and stored Challenger summoners!", status_code=200)

    except Exception as e:
        logging.error(f"Error in FetchChallengerSummoners: {e}")
        return func.HttpResponse(f"Error occurred: {str(e)}", status_code=500)