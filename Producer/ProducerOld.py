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

# Global variable to control the running state
running = True

# Initialize constants for logging
LOG_BACKUP_COUNT = 30  # keep 30 days of logs
LOG_FLUSH_INTERVAL_SECONDS = 24 * 3600  # flush logs every 24 hours

# Determine the execution directory
if getattr(sys, 'frozen', False):
    exe_dir = os.path.dirname(sys.executable)
else:
    exe_dir = os.path.dirname(os.path.abspath(__file__))

# Setup logging directory
logs_dir = os.path.join(exe_dir, 'logs')
if not os.path.exists(logs_dir):
    os.makedirs(logs_dir, exist_ok=True)

# We will create a subdirectory per day (YYYY-MM-DD). Inside that directory,
# we keep a single active file (producer.log) which will be rotated every 6 hours
# producing up to 4 files per day.
current_day = datetime.now().strftime('%Y-%m-%d')

def _make_day_log_dir(day_str: str):
    day_dir = os.path.join(logs_dir, day_str)
    os.makedirs(day_dir, exist_ok=True)
    return day_dir

# Global handler container so we can replace it when day changes
handler = None

# Create/replace the file handler for the current day
def _setup_log_handler_for_day(day_str: str):
    global handler
    day_dir = _make_day_log_dir(day_str)
    log_file_path = os.path.join(day_dir, "producer.log")

    # Remove existing handler if present
    root_logger = logging.getLogger('source_db_logger')
    if handler is not None:
        try:
            root_logger.removeHandler(handler)
            handler.close()
        except Exception:
            pass

    # Rotate every 6 hours (interval=6, when='h')
    new_handler = TimedRotatingFileHandler(
        log_file_path,
        when='M',
        interval=5,
        backupCount=288,
        encoding='utf-8'
    )
    # Suffix will include date and hour so rotated files are like producer.log.YYYY-MM-DD_HH-MM-SS.log
    new_handler.suffix = "%Y-%m-%d_%H-%M-%S.log"
    new_handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s'))

    handler = new_handler
    root_logger.addHandler(handler)

# Initialize logger
source_logger = logging.getLogger('source_db_logger')
source_logger.setLevel(logging.DEBUG)

# run initial setup
_setup_log_handler_for_day(current_day)

# Background thread to watch for day rollover and reconfigure logging to a new folder
def _watch_day_rollover():
    global current_day
    while running:
        now_day = datetime.now().strftime('%Y-%m-%d')
        if now_day != current_day:
            # day changed — set up a new handler for the new day
            try:
                current_day = now_day
                _setup_log_handler_for_day(current_day)
                source_logger.info(f"Log handler moved to new day folder: {current_day}")
            except Exception as e:
                # don't let the watcher thread crash
                print(f"Failed to rotate log handler for new day: {e}")
        # Check once a minute
        for _ in range(60):
            if not running:
                break
            time.sleep(1)

# Ensure logs are flushed to disk periodically (handler.flush() is called on each emit,
# but we also run a daily flush/rotate thread to satisfy the "flush every 1 day" requirement)
def _periodic_log_flush():
    while running:
        try:
            if handler is not None:
                handler.flush()
        except Exception:
            # Avoid raising in the logging thread
            pass
        # Sleep for 24 hours, but wake up earlier if shutting down
        for _ in range(24 * 3600):
            if not running:
                break
            time.sleep(1)

    while running:
        try:
            # flush the handler and trigger rotation check
            handler.flush()
        except Exception:
            # Avoid raising in the logging thread
            pass
        # Sleep for the configured interval
        for _ in range(int(LOG_FLUSH_INTERVAL_SECONDS / 1)):
            if not running:
                break
            time.sleep(1)

# Load configuration
config_file_path = os.path.join(exe_dir, 'config.json')
try:
    with open(config_file_path, 'r') as config_file:
        config = json.load(config_file)
except Exception as e:
    source_logger.error(f"Error loading configuration file: {e}")
    sys.exit(1)

try:
    source_conn_params = config["source_conn_params"]
    kafka_broker = config["kafka_broker"]
    tables = config["tables"]
    producer_id = config.get("producer_id")
    producer_name = config.get("producer_name")
    location_id = config.get("location_id")
except KeyError as e:
    source_logger.error(f"Missing required configuration key: {e}")
    sys.exit(1)

# Signal handling to shut down gracefully
def handle_signal(sig, frame):
    global running
    source_logger.info("Received termination signal, shutting down gracefully...")
    running = False

signal.signal(signal.SIGINT, handle_signal)
signal.signal(signal.SIGTERM, handle_signal)

# Heartbeat thread
def send_heartbeat(producer_id, producer_name, location_id, kafka_broker, interval_seconds=30):
    heartbeat_topic = 'producer_heartbeat'
    kafka_producer = None
    try:
        kafka_producer = KafkaProducer(
            bootstrap_servers=kafka_broker,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

        while running:
            heartbeat_message = {
                'producer_id': producer_id,
                'producer_name': producer_name,
                'location_id': location_id,
                'timestamp': datetime.now().isoformat()
            }
            try:
                kafka_producer.send(heartbeat_topic, heartbeat_message)
                kafka_producer.flush(timeout=10)
                source_logger.debug(f"Sent heartbeat: {heartbeat_message}")
            except Exception as e:
                source_logger.error(f"Failed to send heartbeat: {e}")

            for _ in range(int(interval_seconds / 1)):
                if not running:
                    break
                time.sleep(1)
    except Exception as e:
        source_logger.error(f"Heartbeat thread failed to start: {e}")
    finally:
        if kafka_producer:
            try:
                kafka_producer.close()
            except Exception:
                pass

# Function to fetch and send data from the database to Kafka
def fetch_and_send_data(table_name, check_dbstatus=False, exclude_columns=None):
    if exclude_columns is None:
        exclude_columns = []

    source_logger.debug(f"Connecting to source database for table {table_name}")
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
            record = {}
            for col, val in zip(columns, row):
                if col in exclude_columns:
                    continue
                if isinstance(val, datetime):
                    record[col] = val.isoformat()
                else:
                    try:
                        # pyodbc may return Decimal/other types - let json handle or convert
                        record[col] = val
                    except Exception:
                        record[col] = str(val)

            # Send record to kafka
            try:
                kafka_producer.send(table_name, record)
                source_logger.debug(f"Sent record to Kafka: {record}")
            except Exception as e:
                source_logger.error(f"Failed to send record to Kafka: {e}")

            # Update issync flag using primary key
            try:
                primary_key_column = 'ResultID' if 'ResultID' in columns else columns[0]
                if primary_key_column in record and record[primary_key_column] is not None:
                    update_query = f"UPDATE {table_name} SET issync = 1 WHERE {primary_key_column} = ?"
                    cursor.execute(update_query, (record[primary_key_column],))
                    conn.commit()
                    source_logger.debug(f"Updated issync for record with {primary_key_column}: {record[primary_key_column]}")
                else:
                    source_logger.debug(f"Primary key {primary_key_column} missing in record; skipping issync update")
            except Exception as e:
                source_logger.error(f"Failed to update issync for record: {e}")

        kafka_producer.flush()
        source_logger.info(f"Successfully fetched and sent data from {table_name}")
    except Exception as e:
        source_logger.error(f"Error fetching or sending data from {table_name}: {e}")
    finally:
        try:
            if cursor:
                cursor.close()
        except Exception:
            pass
        try:
            if conn:
                conn.close()
        except Exception:
            pass
        try:
            if kafka_producer:
                kafka_producer.close()
        except Exception:
            pass


# Main application loop
def main():
    source_logger.info("Starting main producer process")

    # Start periodic log flush thread
    flush_thread = threading.Thread(target=_periodic_log_flush, daemon=True)
    flush_thread.start()

    # Start heartbeat thread
    heartbeat_thread = threading.Thread(
        target=send_heartbeat,
        args=(producer_id, producer_name, location_id, kafka_broker),
        daemon=True
    )
    heartbeat_thread.start()

    try:
        while running:
            for table_name, table_config in tables.items():
                fetch_and_send_data(
                    table_name,
                    check_dbstatus=table_config.get("check_dbstatus", False),
                    exclude_columns=table_config.get("exclude_columns", [])
                )
            wait_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            source_logger.debug(f"Waiting for 5 seconds before the next cycle at {wait_timestamp}")
            time.sleep(5)
    except Exception as e:
        source_logger.error(f"Fatal error in main loop: {e}")
    finally:
        source_logger.info("Shutting down producer")
        # Allow threads to see running=False and cleanup
        time.sleep(1)


if __name__ == "__main__":
    # Run the script normally via main()
    main()