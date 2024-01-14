from google.cloud import bigquery
from google.oauth2 import service_account
import mysql.connector

# Function to read MySQL schema
def read_mysql_schema(table_name, mysql_connection):
    cursor = mysql_connection.cursor()
    cursor.execute(f"DESCRIBE {table_name};")
    mysql_schema = cursor.fetchall()
    cursor.close()
    return mysql_schema

# Function to convert MySQL schema to BigQuery format
def convert_to_bigquery_schema(mysql_schema):
    bigquery_schema = []
    for field in mysql_schema:
        # Customize the mapping between MySQL types and BigQuery types as needed
        bigquery_field = {
            'name': field[0],
            'type': 'STRING' if 'varchar' in field[1] else 'INTEGER',  # Example mapping
            'mode': 'NULLABLE',
        }
        bigquery_schema.append(bigquery_field)
    return bigquery_schema

# Set up MySQL connection
mysql_config = {
    'user': 'sa',
    'password': 'reallyStrongPwd123',
    'host': 'localhost',#'172.17.0.2',
    'port': 1433,
    'database': 'BikeStores',
}
mysql_connection = mysql.connector.connect(**mysql_config)

# Set up BigQuery client
credentials = service_account.Credentials.from_service_account_file(
    'credentials.json',
    scopes=["https://www.googleapis.com/auth/cloud-platform"],
)
client = bigquery.Client(credentials=credentials, project='big-data-project-410313')

# List of your table names
your_table_names = ['production.brands', 'sales.orders']

# Iterate through tables
for table_name in your_table_names:
    # Read MySQL schema and convert to BigQuery format
    mysql_schema = read_mysql_schema(table_name, mysql_connection)
    bigquery_schema = convert_to_bigquery_schema(mysql_schema)

    # Create BigQuery table
    dataset_id = 'big-data-project-410313.terraform_dataset'
    table_ref = client.dataset(dataset_id).table(table_name)
    table = bigquery.Table(table_ref, schema=bigquery_schema)
    
    try:
        client.create_table(table)
        print(f"Table {table_name} created successfully.")
    except Exception as e:
        print(f"Error creating table {table_name}: {e}")

print("All tables created successfully.")
