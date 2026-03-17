import json
import pyodbc
import logging
import os
from kafka import KafkaProducer
from datetime import datetime, timedelta
import time
import sys
import signal
import threading
from logging.handlers import TimedRotatingFileHandler
from datetime import datetime, date
from decimal import Decimal
# import win32serviceutil
# import win32service
# import win32event
# import servicemanager


# -----------------------------
# Global variable to control running state
# -----------------------------
running = True

DELETE_LOGS_OLDER_THAN_HOURS = 8
LOG_DELETE_INTERVAL_SECONDS = 24 * 60 * 60  # 1 day


# -----------------------------
# Signal handling
# -----------------------------
def handle_signal(sig, frame):
    global running
    source_logger.info("Received termination signal, shutting down gracefully...")
    print("\nShutting down gracefully...")
    running = False

signal.signal(signal.SIGINT, handle_signal)
signal.signal(signal.SIGTERM, handle_signal)

# -----------------------------
# Determine execution directory
# -----------------------------
if getattr(sys, 'frozen', False):
    exe_dir = os.path.dirname(sys.executable)
else:
    exe_dir = os.path.dirname(os.path.abspath(__file__))

# -----------------------------
# Setup logging
# -----------------------------
logs_dir = os.path.join(exe_dir, 'logs')
os.makedirs(logs_dir, exist_ok=True)
log_file_path = os.path.join(logs_dir, "producer.log")

handler = TimedRotatingFileHandler(
    log_file_path,
    when="H",
    interval=1,
    backupCount=0,   # keep all rotated logs
    encoding="utf-8"
)
handler.suffix = "%Y%m%d_%H.log"

source_logger = logging.getLogger('source_db_logger')
source_logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
handler.setFormatter(formatter)
source_logger.addHandler(handler)

# -----------------------------
# Load configuration
# -----------------------------
config_file_path = os.path.join(exe_dir, 'config.json')
try:
    with open(config_file_path, 'r') as config_file:
        config = json.load(config_file)
except Exception as e:
    source_logger.error(f"Error loading configuration file: {e}")
    print(f"Error loading configuration file: {e}")
    sys.exit(1)

try:
    source_conn_params = config["source_conn_params"]
    kafka_broker = config["kafka_broker"]
    tables = config["tables"]
    producer_id = config["producer_id"]
    producer_name = config["producer_name"]
    location_id = config["location_id"]
except KeyError as e:
    source_logger.error(f"Missing required configuration key: {e}")
    print(f"Missing required configuration key: {e}")
    sys.exit(1)

# -----------------------------
# Json Serialization
# -----------------------------

def json_serializer(obj):
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, Decimal):
        return float(obj)
    return obj


# -----------------------------
# Heartbeat producer
# -----------------------------
def send_heartbeat(producer_id, producer_name, location_id, kafka_producer):
    heartbeat_topic = 'producer_heartbeat'

    while running:
        heartbeat_message = {
            'producer_id': producer_id,
            'producer_name': producer_name,
            'location_id': location_id,
            'timestamp': datetime.now().isoformat()
        }

        kafka_producer.send(heartbeat_topic, heartbeat_message)
        source_logger.debug(f"Sent heartbeat: {heartbeat_message}")
        print(f"Sent heartbeat: {heartbeat_message}")

        time.sleep(10)  # heartbeat every 10 seconds


# -----------------------------
# Data fetch and send
# -----------------------------
def fetch_and_send_data(table_name, kafka_producer, check_dbstatus=False, exclude_columns=None):
    exclude_columns = exclude_columns or []

    source_logger.debug(f"Connecting to source database for table {table_name}")
    print(f"Connecting to source database for table {table_name}")

    conn = None
    cursor = None

    try:
        conn_str = (
            f"DRIVER={{ODBC Driver 13 for SQL Server}};"
            f"SERVER={source_conn_params['host']},{source_conn_params['port']};"
            f"DATABASE={source_conn_params['dbname']};"
            f"UID={source_conn_params['user']};"
            f"PWD={source_conn_params['password']}"
        )
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        source_logger.info(f"Connected to source database to fetch data from {table_name}")
        print(f"Connected to source database to fetch data from {table_name}")

        three_days_ago = (datetime.now() - timedelta(days=3)).strftime('%Y-%m-%d')

        if table_name == "dbo.Patient_Details":
            query = f"SELECT * FROM {table_name} WHERE issync = 0 AND CreateDate >= '{three_days_ago}'"
        elif table_name in ["dbo.Orders", "dbo.Test_Parameters"]:
            query = f"SELECT * FROM {table_name} WHERE issync = 0 AND CreatedDate >= '{three_days_ago}'"
        elif table_name == "dbo.UtilityException":
            query = f"SELECT * FROM {table_name} WHERE issync = 0 AND Timestamp >= '{three_days_ago}'"
        else:
            query = f"SELECT * FROM {table_name} WHERE issync = 0"

        if check_dbstatus:
            query += " AND DbStatus BETWEEN 1 AND 5"

        cursor.execute(query)
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]

        for row in rows:
            record = {
                col: (val.isoformat() if isinstance(val, datetime) else val)
                for col, val in zip(columns, row)
                if col not in exclude_columns
            }

            future = kafka_producer.send(table_name, record)
            future.get(timeout=10)  # wait for ACK

            source_logger.debug(f"Sent record to Kafka: {record}")
            print(f"Sent record to Kafka: {record}")

            primary_key_column = 'ResultID' if 'ResultID' in columns else columns[0]
            update_query = f"UPDATE {table_name} SET issync = 1 WHERE {primary_key_column} = ?"

            cursor.execute(update_query, (record[primary_key_column],))
            conn.commit()

            source_logger.debug(f"Updated issync for record with {primary_key_column}: {record[primary_key_column]}")
            print(f"Updated issync for record with {primary_key_column}: {record[primary_key_column]}")

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


def delete_old_logs():
    source_logger.info("Daily log deletion thread started")

    while running:
        try:
            cutoff_time = datetime.now() - timedelta(
                hours=DELETE_LOGS_OLDER_THAN_HOURS
            )

            for filename in os.listdir(logs_dir):
                file_path = os.path.join(logs_dir, filename)

                # Skip non-files
                if not os.path.isfile(file_path):
                    continue

                # Do NOT delete active log file
                if filename == "producer.log":
                    continue

                file_mtime = datetime.fromtimestamp(os.path.getmtime(file_path))

                if file_mtime < cutoff_time:
                    try:
                        os.remove(file_path)
                        source_logger.info(f"Deleted old log file: {filename}")
                    except Exception as e:
                        source_logger.error(
                            f"Failed to delete log {filename}: {e}"
                        )

        except Exception as e:
            source_logger.error(f"Daily log cleanup failed: {e}")

        # Sleep for 24 hours (safe shutdown)
        for _ in range(LOG_DELETE_INTERVAL_SECONDS):
            if not running:
                source_logger.info("Log deletion thread stopping")
                return
            time.sleep(1)


# -----------------------------
# Main application
# -----------------------------
if __name__ == "__main__":

    # 1. Create a single producer for heartbeat
    heartbeat_producer = KafkaProducer(
        bootstrap_servers=kafka_broker,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    # 2. Create a single producer for all table data
    data_producer = KafkaProducer(
        bootstrap_servers=kafka_broker,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    # Start heartbeat thread
    threading.Thread(
        target=send_heartbeat,
        args=(producer_id, producer_name, location_id, heartbeat_producer),
        daemon=True
    ).start()

    # Start log deletion thread
    threading.Thread(
        target=delete_old_logs,
        daemon=True
    ).start()

    # Main loop for fetching and sending data
    while running:
        for table_name, table_config in tables.items():
            fetch_and_send_data(
                table_name,
                kafka_producer=data_producer,
                check_dbstatus=table_config.get("check_dbstatus", False),
                exclude_columns=table_config.get("exclude_columns", [])
            )

        wait_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        source_logger.debug(f"Waiting for 5 seconds before the next cycle at {wait_timestamp}")
        print(f"Waiting for 5 seconds before the next cycle at {wait_timestamp}")
        time.sleep(5)

    # Flush and close producers on shutdown
    data_producer.flush()
    data_producer.close()
    heartbeat_producer.flush()
    heartbeat_producer.close()

    source_logger.info("Process terminated gracefully.")
    print("Process terminated gracefully.")



