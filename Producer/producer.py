check this 

import json
import pyodbc
import logging
import os
from kafka import KafkaProducer
from kafka import KafkaConsumer
from datetime import datetime, timedelta, date
import time
import sys
import signal
import threading
from logging.handlers import TimedRotatingFileHandler
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

        three_days_ago = (datetime.now() - timedelta(days=3)).strftime('%Y-%m-%d')

        # Build query
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

        if not rows:
            source_logger.info(f"No records found for {table_name}")
            return

        columns = [desc[0] for desc in cursor.description]

        source_logger.info(f"Fetched {len(rows)} records from {table_name}")

        for row in rows:
            try:
                record = {
                    col: val
                    for col, val in zip(columns, row)
                    if col not in exclude_columns
                }

                json_data = json.dumps(record, default=json_serializer)
                source_logger.debug(f"SENDING: {json_data}")

                # Send to Kafka
                future = kafka_producer.send(table_name, record)
                future.get(timeout=10)

                source_logger.debug(f"Sent record successfully")

                # Small delay to avoid flooding
                time.sleep(0.02)

            except Exception as e:
                source_logger.error(f"Error sending single record: {e}")

        source_logger.info(f"Completed sending data for {table_name}")

    except Exception as e:
        source_logger.error(f"Error fetching or sending data from {table_name}: {e}")
        print(f"Error fetching or sending data from {table_name}: {e}")

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


# -----------------------------
# ACK Receiver
# -----------------------------
def ack_receiver():
    source_logger.info("ACK receiver started")

    while running:
        consumer = None
        conn = None
        cursor = None

        try:
            # -----------------------------
            # Kafka Consumer Setup
            # -----------------------------
            consumer = KafkaConsumer(
                'ack_topic',
                bootstrap_servers=kafka_broker,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                group_id=f"ack_group_{producer_id}",
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                session_timeout_ms=30000,
                heartbeat_interval_ms=10000
            )

            source_logger.info("Connected to Kafka ACK topic")

            # -----------------------------
            # DB Connection (LOCAL DB)
            # -----------------------------
            # conn_str = (
            #     "DRIVER={ODBC Driver 18 for SQL Server};"
            #     "SERVER=localhost,1433;"
            #     "DATABASE=PathKind_BPHT;"
            #     "UID=sa;"
            #     "PWD=Labsoul@2024;"
            #     "Encrypt=no;"
            #     "TrustServerCertificate=yes;"
            # )
            #
            # conn = pyodbc.connect(conn_str, timeout=5)
            # cursor = conn.cursor()

            conn_str = (
                f"DRIVER={{ODBC Driver 13 for SQL Server}};"
                f"SERVER=localhost,1433;"
                f"DATABASE=PathKind_BPHT;"
                f"UID=sa;"
                f"PWD=Labsoul@2024;"
            )
            conn = pyodbc.connect(conn_str, timeout=5)
            cursor = conn.cursor()
            
            source_logger.info("Connected to LOCAL DB for ACK updates")

            # -----------------------------
            # Consume ACK Messages
            # -----------------------------
            for message in consumer:
                if not running:
                    break

                try:
                    ack = message.value
                    source_logger.debug(f"ACK RECEIVED: {ack}")

                    result_id = ack.get("ResultID")
                    table_name = ack.get("table_name")
                    primary_key = ack.get("primary_key", "ResultID")

                    # Validation
                    if not result_id or not table_name:
                        source_logger.warning(f"Invalid ACK message skipped: {ack}")
                        continue

                    # Update Local DB
                    query = f"UPDATE {table_name} SET issync = 1 WHERE {primary_key} = ?"
                    cursor.execute(query, (result_id,))
                    conn.commit()

                    source_logger.info(f"ACK processed successfully for {table_name}:{result_id}")

                except Exception as e:
                    source_logger.error(f"Error processing ACK message: {e}")

        except Exception as e:
            source_logger.error(f"ACK receiver main loop error: {e}")

        finally:
            # -----------------------------
            # Cleanup resources
            # -----------------------------
            try:
                if cursor:
                    cursor.close()
                if conn:
                    conn.close()
                if consumer:
                    consumer.close()
            except Exception as cleanup_error:
                source_logger.error(f"Error during cleanup: {cleanup_error}")

        # -----------------------------
        # Retry mechanism (avoid crash loop)
        # -----------------------------
        if running:
            source_logger.info("Reconnecting ACK receiver in 5 seconds...")
            time.sleep(5)


# -----------------------------
# Main application
# -----------------------------
if __name__ == "__main__":

    # 1. Create a single producer for heartbeat
    heartbeat_producer = KafkaProducer(
        bootstrap_servers=kafka_broker,
        value_serializer=lambda v: json.dumps(v, default=json_serializer).encode('utf-8')
    )

    # 2. Create a single producer for all table data
    data_producer = KafkaProducer(
        bootstrap_servers=kafka_broker,
        value_serializer=lambda v: json.dumps(v, default=json_serializer).encode('utf-8')
    )

    # Start heartbeat thread
    threading.Thread(
        target=send_heartbeat,
        args=(producer_id, producer_name, location_id, heartbeat_producer),
        daemon=True
    ).start()

    # Ack thread
    threading.Thread(
        target=ack_receiver,
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
