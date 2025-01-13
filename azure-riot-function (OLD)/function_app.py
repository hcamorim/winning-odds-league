import azure.functions as func
from azure.identity import DefaultAzureCredential
import pyodbc
import logging

# Azure SQL Database details
server = "myriotdataserver.database.windows.net"
database = "myRiotDataSQL"

# Function App definition
app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

@app.route(route="test-sql-connection")
def test_sql_connection(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Connecting to Azure SQL Database using Managed Identity...")

    try:
        # Use DefaultAzureCredential to get an access token
        credential = DefaultAzureCredential()
        access_token = credential.get_token("https://database.windows.net/").token

        # Build the connection string without using the 'Authentication' attribute
        conn_str = (
            f"Driver={{ODBC Driver 18 for SQL Server}};"
            f"Server={server};"
            f"Database={database};"
            f"TrustServerCertificate=yes;"
        )

        # Connect to the database using the access token
        with pyodbc.connect(conn_str, attrs_before={1138: access_token}) as conn:  # 1138 = SQL_COPT_SS_ACCESS_TOKEN
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            row = cursor.fetchone()
            if row:
                logging.info("Connected to the database!")
                return func.HttpResponse("Database connection successful.")
            else:
                return func.HttpResponse("Connection test failed.", status_code=500)

    except Exception as e:
        logging.error(f"Database connection error: {e}")
        return func.HttpResponse(f"Database connection error: {e}", status_code=500)