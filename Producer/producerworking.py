import json
import pyodbc
import logging
import os
from kafka import KafkaProducer
from datetime import datetime, timedelta
import time
import sys
import signal
from flask import Flask, jsonify
from flask_cors import CORS
import threading
from logging.handlers import TimedRotatingFileHandler
import glob
# <-- add this import at the top with others
# used for creating log files

# Global variable to control the running state
running = True

# Initialize Flask app
app = Flask(__name__)
CORS(app)

# Signal handling to shut down gracefully
def handle_signal(sig, frame):
    global running
    source_logger.info("Received termination signal, shutting down gracefully...")
    print("\nShutting down gracefully...")
    running = False

signal.signal(signal.SIGINT, handle_signal)
signal.signal(signal.SIGTERM, handle_signal)

# Determine the execution directory
if getattr(sys, 'frozen', False):
    exe_dir = os.path.dirname(sys.executable)
else:
    exe_dir = os.path.dirname(os.path.abspath(__file__))

# Setup logging
logs_dir = os.path.join(exe_dir, 'logs')
if not os.path.exists(logs_dir):
    os.makedirs(logs_dir)

# Create timed rotating file handler (hourly rotation)
log_file_path = os.path.join(logs_dir, "producer.log")
handler = TimedRotatingFileHandler(
    log_file_path, when="H", interval=hours_per_file, backupCount=days_to_keep*24, encoding="utf-8"
)
handler.suffix = "%Y%m%d_%H%M%S.log"  # timestamp suffix

source_logger = logging.getLogger('source_db_logger')
source_logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
handler.setFormatter(formatter)
source_logger.addHandler(handler)

source_logger.info(f"Hourly log rotation initialized with {hours_per_file} hour(s) per file, keeping logs for {days_to_keep} days")
print(f"Hourly log rotation initialized with {hours_per_file} hour(s) per file, keeping logs for {days_to_keep} days")

# Load configuration
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

# Function to send heartbeat messages
def send_heartbeat(producer_id, producer_name, location_id, kafka_broker):
    heartbeat_topic = 'producer_heartbeat'
    kafka_producer = KafkaProducer(
        bootstrap_servers=kafka_broker,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    # Get log rotation parameters
    try:
        log_rotation_config = config.get("log_rotation", {})
        days_to_keep = log_rotation_config.get("days_to_keep", 8)
    except:
        days_to_keep = 8

    # Track when we last cleaned logs
    last_cleanup_time = time.time()

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

        # Periodically clean old logs (every 6 hours)
        current_time = time.time()
        if current_time - last_cleanup_time > (6 * 60 * 60):  # 6 hours
            clean_old_logs(logs_dir, days_to_keep)
            last_cleanup_time = current_time

        time.sleep(30)  # Send heartbeat every 30 seconds

    kafka_producer.close()

# Function to fetch and send data from the database to Kafka
def fetch_and_send_data(table_name, check_dbstatus=False, exclude_columns=None):
    source_logger.debug(f"Connecting to source database for table {table_name}")
    print(f"Connecting to source database for table {table_name}")
    conn = None
    cursor = None
    kafka_producer = None
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

        seven_days_ago = (datetime.now() - timedelta(days=3)).strftime('%Y-%m-%d')
        if table_name == "dbo.Patient_Details":
            query = f"SELECT * FROM {table_name} WHERE (issync = 0) AND CreateDate >= '{seven_days_ago}'"
        elif table_name in ["dbo.Orders", "dbo.Test_Parameters"]:
            query = f"SELECT * FROM {table_name} WHERE (issync = 0) AND CreatedDate >= '{seven_days_ago}'"
        elif table_name == "dbo.UtilityException":
            query = f"SELECT * FROM {table_name} WHERE (issync = 0) AND Timestamp >= '{seven_days_ago}'"
        else:
            query = f"SELECT * FROM {table_name} WHERE (issync = 0)"

        if check_dbstatus:
            query += " AND DbStatus BETWEEN 1 AND 5"

        cursor.execute(query)
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]

        kafka_producer = KafkaProducer(
            bootstrap_servers=kafka_broker,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

        for row in rows:
            record = {col: (val.isoformat() if isinstance(val, datetime) else val)
                      for col, val in zip(columns, row) if col not in exclude_columns}
            kafka_producer.send(table_name, record)
            source_logger.debug(f"Sent record to Kafka: {record}")
            print(f"Sent record to Kafka: {record}")

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
    # Run the script directly (standalone application)
    source_logger.debug("Starting data fetch and send process")
    print("Starting data fetch and send process")

    # Start the heartbeat thread
    threading.Thread(target=send_heartbeat, args=(producer_id, producer_name, location_id, kafka_broker), daemon=True).start()

    # Get log rotation parameters
    try:
        log_rotation_config = config.get("log_rotation", {})
        days_to_keep = log_rotation_config.get("days_to_keep", 8)
    except:
        days_to_keep = 8

    # Track when we last cleaned logs
    last_cleanup_time = time.time()

    while running:
        for table_name, table_config in tables.items():
            fetch_and_send_data(
                table_name,
                check_dbstatus=table_config.get("check_dbstatus", False),
                exclude_columns=table_config.get("exclude_columns", [])
            )

        # Periodically clean old logs (every 6 hours)
        current_time = time.time()
        if current_time - last_cleanup_time > (6 * 60 * 60):  # 6 hours
            clean_old_logs(logs_dir, days_to_keep)
            last_cleanup_time = current_time

        wait_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        source_logger.debug(f"Waiting for 5 seconds before the next cycle at {wait_timestamp}")
        print(f"Waiting for 5 seconds before the next cycle at {wait_timestamp}")
        time.sleep(5)

    source_logger.info("Shut down gracefully.")
    print("Process terminated gracefully.")