import json
import pyodbc
import logging
import os
from kafka import KafkaProducer
from datetime import datetime, timedelta, date
import time
import sys
import signal
import threading
from logging.handlers import TimedRotatingFileHandler
from decimal import Decimal

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
    backupCount=0,
    encoding="utf-8"
)
handler.suffix = "%Y%m%d_%H.log"

source_logger = logging.getLogger('source_db_logger')
source_logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
handler.setFormatter(formatter)

if not source_logger.handlers:
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
# JSON Serialization
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
        try:
            heartbeat_message = {
                'producer_id': producer_id,
                'producer_name': producer_name,
                'location_id': location_id,
                'timestamp': datetime.now().isoformat()
            }

            kafka_producer.send(heartbeat_topic, heartbeat_message)
            source_logger.debug(f"Sent heartbeat: {heartbeat_message}")
            print(f"Sent heartbeat: {heartbeat_message}")

        except Exception as e:
            source_logger.error(f"Heartbeat send failed: {e}")
            print(f"Heartbeat send failed: {e}")

        time.sleep(10)

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
            query = f"""
                SELECT * FROM {table_name}
                WHERE IsSync = 0
                  AND CreateDate >= '{three_days_ago}'
            """

        elif table_name == "dbo.Orders":
            query = f"""
                SELECT * FROM {table_name}
                WHERE IsSync = 0
                  AND CreatedDate >= '{three_days_ago}'
            """

        elif table_name == "dbo.Test_Parameters":
            query = f"""
                SELECT * FROM {table_name}
                WHERE IsSync = 0
                  AND DbStatus IN (1, 5)
            """

        elif table_name == "dbo.UtilityException":
            query = f"""
                SELECT * FROM {table_name}
                WHERE IsSync = 0
                  AND Timestamp >= '{three_days_ago}'
            """

        else:
            query = f"SELECT * FROM {table_name} WHERE IsSync = 0"

        # Keep this only for other tables, not Test_Parameters
        if check_dbstatus and table_name != "dbo.Test_Parameters":
            query += " AND DbStatus BETWEEN 1 AND 5"

        source_logger.debug(f"Executing query for {table_name}: {query}")
        print(f"Executing query for {table_name}: {query}")

        cursor.execute(query)
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]

        for row in rows:
            try:
                record = {
                    col: val
                    for col, val in zip(columns, row)
                    if col not in exclude_columns
                }

                primary_key_column = 'ResultID' if 'ResultID' in columns else columns[0]
                primary_key_value = record[primary_key_column]

                # remove internal columns before sending to Kafka
                record_to_send = dict(record)
                record_to_send.pop("IsSync", None)
                record_to_send.pop("IsJsonCreated", None)

                # 1. Create JSON first
                json_data = json.dumps(record_to_send, default=json_serializer)
                source_logger.info(f"ACTUAL JSON CREATED: {json_data}")
                print(f"ACTUAL JSON CREATED: {json_data}")

                # 2. Mark IsJsonCreated = 1 after JSON creation
                update_json_query = f"""
                    UPDATE {table_name}
                    SET IsJsonCreated = 1
                    WHERE {primary_key_column} = ?
                      AND IsSync = 0
                """
                cursor.execute(update_json_query, (primary_key_value,))
                conn.commit()

                source_logger.debug(
                    f"Marked IsJsonCreated=1 for {primary_key_column}: {primary_key_value}"
                )
                print(
                    f"Marked IsJsonCreated=1 for {primary_key_column}: {primary_key_value}"
                )

                # 3. Send to Kafka
                future = kafka_producer.send(table_name, record_to_send)

                # 4. Wait for broker ACK
                future.get(timeout=10)

                source_logger.debug(f"Broker ACK received for record: {primary_key_value}")
                print(f"Broker ACK received for record: {primary_key_value}")

                # 5. Mark IsSync = 1 only after broker ACK
                update_sync_query = f"""
                    UPDATE {table_name}
                    SET IsSync = 1
                    WHERE {primary_key_column} = ?
                      AND IsJsonCreated = 1
                """
                cursor.execute(update_sync_query, (primary_key_value,))
                conn.commit()

                source_logger.debug(
                    f"Marked IsSync=1 after broker ACK for {primary_key_column}: {primary_key_value}"
                )
                print(
                    f"Marked IsSync=1 after broker ACK for {primary_key_column}: {primary_key_value}"
                )

            except Exception as row_error:
                source_logger.error(
                    f"Row processing failed for table {table_name}: {row_error}"
                )
                print(f"Row processing failed for table {table_name}: {row_error}")

        source_logger.info(f"Successfully fetched and processed data from {table_name}")
        print(f"Successfully fetched and processed data from {table_name}")

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

                if not os.path.isfile(file_path):
                    continue

                if filename == "producer.log":
                    continue

                file_mtime = datetime.fromtimestamp(os.path.getmtime(file_path))

                if file_mtime < cutoff_time:
                    try:
                        os.remove(file_path)
                        source_logger.info(f"Deleted old log file: {filename}")
                    except Exception as e:
                        source_logger.error(f"Failed to delete log {filename}: {e}")

        except Exception as e:
            source_logger.error(f"Daily log cleanup failed: {e}")

        for _ in range(LOG_DELETE_INTERVAL_SECONDS):
            if not running:
                source_logger.info("Log deletion thread stopping")
                return
            time.sleep(1)

# -----------------------------
# Main application
# -----------------------------
if __name__ == "__main__":

    heartbeat_producer = KafkaProducer(
        bootstrap_servers=kafka_broker,
        value_serializer=lambda v: json.dumps(v, default=json_serializer).encode('utf-8')
    )

    data_producer = KafkaProducer(
        bootstrap_servers=kafka_broker,
        value_serializer=lambda v: json.dumps(v, default=json_serializer).encode('utf-8')
    )

    threading.Thread(
        target=send_heartbeat,
        args=(producer_id, producer_name, location_id, heartbeat_producer),
        daemon=True
    ).start()

    threading.Thread(
        target=delete_old_logs,
        daemon=True
    ).start()

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

    data_producer.flush()
    data_producer.close()
    heartbeat_producer.flush()
    heartbeat_producer.close()

    source_logger.info("Process terminated gracefully.")
    print("Process terminated gracefully.")