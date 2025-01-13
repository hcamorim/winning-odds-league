import azure.functions as func
import mysql.connector
from prettytable import PrettyTable
import logging

# Key Vault setup
vault_url = "https://myriotdatakeyvault.vault.azure.net/"
credential = DefaultAzureCredential()
client = SecretClient(vault_url=vault_url, credential=credential)

# Get SQL password from Key Vault
password = client.get_secret("FunctionAppUserPassword").value

# Azure SQL Database connection details
server = "myriotdataserver.database.windows.net"
database = "myRiotDataSQL"
username = "myRiotDataFunctionApp"

# Define a new FunctionApp instance
app = func.FunctionApp()

@app.route(route="create-test-table")
def create_test_table(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Connecting to Azure SQL Database...')
    try:
        connection = mysql.connector.connect(
            host=server,
            database=database,
            user=username,
            password=password
        )
        if connection.is_connected():
            cursor = connection.cursor()
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS TestTable (
                test1 NVARCHAR(255),
                test2 NVARCHAR(255),
                test3 NVARCHAR(255),
                test4 NVARCHAR(255)
            );
            """)
            connection.commit()

            table = PrettyTable()
            table.field_names = ["Test1", "Test2", "Test3", "Test4"]
            cursor.execute("SELECT * FROM TestTable")
            for row in cursor.fetchall():
                table.add_row(row)

            return func.HttpResponse(f"<pre>{table}</pre>", status_code=200, mimetype="text/html")

    except mysql.connector.Error as e:
        logging.error(f"Database Error: {e}")
        return func.HttpResponse(f"Database Error: {e}", status_code=500)
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()