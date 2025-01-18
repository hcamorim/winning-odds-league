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
    logging.info('FetchTopSummoners function processing a request.')

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

        # Regions and ranks to fetch
        regions = ["euw1", "eun1", "kr", "na1"]
        ranks = [
            ("challenger", "Challenger"),
            ("grandmaster", "Grandmaster"),
        ]

        # Fetch summoner data from Riot API
        summoners = []
        base_url = "https://{region}.api.riotgames.com/lol/league/v4/{rank}leagues/by-queue/RANKED_SOLO_5x5"
        for region in regions:
            for api_rank, rank_label in ranks:
                url = base_url.format(region=region, rank=api_rank)
                response = requests.get(url, headers=headers)

                if response.status_code == 429:  # Rate limit exceeded
                    retry_after = int(response.headers.get("Retry-After", 60))
                    logging.warning(f"Rate limit exceeded. Retrying after {retry_after} seconds...")
                    time.sleep(retry_after)
                    continue

                if response.status_code != 200:
                    raise Exception(f"Failed to fetch {rank_label} summoners for {region}: {response.text}")

                league_data = response.json()
                if "entries" not in league_data:
                    raise Exception(f"Unexpected API response format: {league_data}")

                for entry in league_data["entries"]:
                    summoners.append({
                        "summonerID": entry["summonerId"],
                        "rank": rank_label,
                        "region": region
                    })

        # Ensure summoners are fetched
        if not summoners:
            logging.info("No summoners fetched from the API.")
            return func.HttpResponse("No summoners fetched from the API.", status_code=204)

        # Update SQL database
        with connect_to_database(connection_string) as conn:
            cursor = conn.cursor()

            # Ensure the Summoners table exists
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

            # Create a temporary table for new data
            cursor.execute("DROP TABLE IF EXISTS #TempSummoners;")
            cursor.execute("""
            CREATE TABLE #TempSummoners (
                SummonerID VARCHAR(100) NOT NULL,
                Rank VARCHAR(50) NOT NULL,
                Region VARCHAR(10) NOT NULL
            );
            """)

            # Insert new data into the temporary table
            cursor.executemany(
                """
                INSERT INTO #TempSummoners (SummonerID, Rank, Region)
                VALUES (?, ?, ?);
                """,
                [(s["summonerID"], s["rank"], s["region"]) for s in summoners]
            )

            # Merge data into the main table
            cursor.execute("""
            MERGE INTO Summoners AS Target
            USING #TempSummoners AS Source
            ON Target.SummonerID = Source.SummonerID AND Target.Region = Source.Region
            WHEN MATCHED THEN
                UPDATE SET Rank = Source.Rank
            WHEN NOT MATCHED THEN
                INSERT (SummonerID, Rank, Region)
                VALUES (Source.SummonerID, Source.Rank, Source.Region);
            """)

            # Remove outdated entries
            cursor.execute("""
            DELETE FROM Summoners
            WHERE NOT EXISTS (
                SELECT 1
                FROM #TempSummoners AS Temp
                WHERE Summoners.SummonerID = Temp.SummonerID
                AND Summoners.Region = Temp.Region
            );
            """)

        return func.HttpResponse("Summoners table updated successfully!", status_code=200)

    except Exception as e:
        logging.error(f"Error in FetchTopSummoners: {e}")
        return func.HttpResponse(f"Error occurred: {str(e)}", status_code=500)