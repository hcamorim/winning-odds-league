import os
import pyodbc
import azure.functions as func
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
import logging

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('WriteMyRiotMatches function processing a request.')
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

        # Connect & create table if not exists
        with pyodbc.connect(connection_string, autocommit=True) as conn:
            cursor = conn.cursor()
            create_table_sql = """
            IF NOT EXISTS (
                SELECT *
                FROM sys.tables
                WHERE name = 'myRiotMatches'
            )
            BEGIN
                CREATE TABLE myRiotMatches (
                    ID INT IDENTITY(1,1) PRIMARY KEY,
                    MatchID INT NOT NULL,
                    PlayerName VARCHAR(100) NOT NULL,
                    Score INT NOT NULL
                );
            END
            """
            cursor.execute(create_table_sql)

            # Insert sample data
            sample_data = [
                (001, 'Player1', 15),
                (002, 'Player2', 30),
                (003, 'Player2', 50)
            ]
            for match_id, player_name, score in sample_data:
                cursor.execute(
                    """
                    INSERT INTO myRiotMatches (MatchID, PlayerName, Score)
                    VALUES (?, ?, ?)
                    """,
                    (match_id, player_name, score)
                )

        return func.HttpResponse(
            "myRiotMatches table created (if not existing) and sample data inserted!",
            status_code=200
        )

    except Exception as e:
        logging.error(f"Error in WriteMyRiotMatches: {e}")
        return func.HttpResponse(
            f"Error occurred: {str(e)}",
            status_code=500
        )