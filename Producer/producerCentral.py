import json
import pyodbc
import logging
import os
from kafka import KafkaProducer
from datetime import datetime
import time

# Ensure the logs directory exists
logs_dir = 'logs'
if not os.path.exists(logs_dir):
    os.makedirs(logs_dir)

# Setup logging
source_logger = logging.getLogger('source_db_logger')
source_logger.setLevel(logging.DEBUG)
handler = logging.FileHandler(os.path.join(logs_dir, 'source_db.log'))
handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s'))
source_logger.addHandler(handler)

# Source database connection parameters
source_conn_params = {
    "dbname": "PathKind_BPHT",
    "user": "sa",
    "password": "Labsoul@2024",
    "host": "CHANDIGARH\SQLEXPRESS_CHD",
    "port": "1433"
}

# Columns to exclude for each table
exclude_columns = {

    "dbo.Test_Parameters": ['CentralResultID'],

}


def fetch_and_send_data(table_name, check_dbstatus=False):
    source_logger.debug(f"Connecting to source database for table {table_name}")
    print(f"Connecting to source database for table {table_name}")

    conn = None
    cursor = None
    kafka_producer = None

    try:
        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={source_conn_params['host']},{source_conn_params['port']};"
            f"DATABASE={source_conn_params['dbname']};"
            f"UID={source_conn_params['user']};"
            f"PWD={source_conn_params['password']}"
        )

        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        source_logger.info(f"Connected to source database to fetch data from {table_name}")
        print(f"Connected to source database to fetch data from {table_name}")

        # Construct query based on the presence of DbStatus column
        query = f"SELECT * FROM {table_name} WHERE (issync = 0 OR issync IS NULL OR issync = 1 )"
        if check_dbstatus:
            query += " AND DbStatus BETWEEN 1 AND 5"

        cursor.execute(query)
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]

        columns_to_exclude = exclude_columns.get(table_name, [])

        kafka_producer = KafkaProducer(
            bootstrap_servers='localhost:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

        for row in rows:
            record = {col: (val.isoformat() if isinstance(val, datetime) else val)
                      for col, val in zip(columns, row) if col not in columns_to_exclude}

            # Send record to Kafka
            kafka_producer.send(table_name, record)
            source_logger.debug(f"Sent record to Kafka: {record}")
            print(f"Sent record to Kafka: {record}")

            # Update the record as synced
            primary_key_column = 'ResultID' if 'ResultID' in columns else columns[0]
            update_query = f"UPDATE {table_name} SET issync = 1 WHERE {primary_key_column} = ?"
            cursor.execute(update_query, (record[primary_key_column],))
            conn.commit()
            source_logger.debug(f"Updated issync for record with {primary_key_column}: {record[primary_key_column]}")
            print(f"Updated issync for record with {primary_key_column}: {record[primary_key_column]}")

        kafka_producer.flush()
        source_logger.info(f"Successfully fetched and sent data from {table_name}")
        print(f"Successfully fetched and sent data from {table_name}")

    except Exception as e:
        source_logger.error(f"Error fetching or sending data from {table_name}: {e}")
        print(f"Error fetching or sending data from {table_name}: {e}")

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        if kafka_producer:
            kafka_producer.close()


if __name__ == "__main__":
    source_logger.debug("Starting data fetch and send process")
    print("Starting data fetch and send process")

    # Define tables and whether they have the DbStatus column
    tables = {
        "dbo.Patient_Details": False,
        "dbo.Orders": True,
        "dbo.Test_Parameters": True,
        "dbo.ProductAuth_Table": False,
        "dbo.Users": False,
        "dbo.ExtTestCodeConfiguration": False,
        "dbo.LogException": False,
        "dbo.UtilityException": False,
    }

    while True:
        for table_name, has_dbstatus in tables.items():
            fetch_and_send_data(table_name, check_dbstatus=has_dbstatus)

        # Wait for 5 seconds before repeating
        source_logger.debug("Waiting for 5 seconds before the next cycle")
        print("Waiting for 5 seconds before the next cycle")
        time.sleep(5)
