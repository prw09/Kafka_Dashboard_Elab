import json
import pyodbc
import logging
import os
import sys
import time
import signal
import shutil
import threading
from datetime import datetime, timedelta
from kafka import KafkaProducer
from logging.handlers import TimedRotatingFileHandler


# Windows service imports
import win32serviceutil
import win32service
import win32event
import servicemanager


# -------------------------------------------------------
# Global Variables & Constants
# -------------------------------------------------------

running = True

# MAX_LOG_SIZE = 200 * 1024 * 1024  # 200 MB
# LOG_RETENTION_DAILY = 6

LOG_DELETE_INTERVAL_SECONDS = 10 * 60   # check every 10 minutes
DELETE_LOGS_OLDER_THAN_HOURS = 1        # 1 hour (TEST)



# -------------------------------------------------------
# Determine Execution Directory
# -------------------------------------------------------

if getattr(sys, 'frozen', False):
    exe_dir = os.path.dirname(sys.executable)
else:
    exe_dir = os.path.dirname(os.path.abspath(__file__))


# -------------------------------------------------------
# Setup Logging Directory
# -------------------------------------------------------

LOG_RETENTION_HOURS = 8

logs_dir = os.path.join(exe_dir, 'logs')
os.makedirs(logs_dir, exist_ok=True)

handler = None

def setup_hourly_logging():
    global handler

    log_file_path = os.path.join(logs_dir, "producer.log")
    logger = logging.getLogger('source_db_logger')
    logger.setLevel(logging.DEBUG)
    logger.propagate = False

    if handler:
        logger.removeHandler(handler)
        handler.close()

    handler = TimedRotatingFileHandler(
        filename=log_file_path,
        when="H",
        interval=1,
        backupCount=LOG_RETENTION_HOURS,
        encoding="utf-8",
        delay=True
    )

    handler.suffix = "%Y-%m-%d_%H.log"
    handler.setFormatter(
        logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    )

    logger.addHandler(handler)


# -------------------------------------------------------
# Logger Init
# -------------------------------------------------------

source_logger = logging.getLogger('source_db_logger')
setup_hourly_logging()

# -------------------------------------------------------
# Periodic Log Flush Thread
# -------------------------------------------------------

def _periodic_log_flush():
    while running:
        try:
            if handler:
                handler.flush()
        except Exception:
            pass

        # flush every 24 hours
        for _ in range(24 * 3600):
            if not running:
                return
            time.sleep(1)


# -------------------------------------------------------
# Load Config
# -------------------------------------------------------

config_file_path = os.path.join(exe_dir, 'config.json')

try:
    with open(config_file_path, 'r') as f:
        config = json.load(f)
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


# -------------------------------------------------------
# Signal Handler
# -------------------------------------------------------

def handle_signal(sig, frame):
    global running
    source_logger.info("Received termination signal, shutting down...")
    running = False


signal.signal(signal.SIGINT, handle_signal)
signal.signal(signal.SIGTERM, handle_signal)


# -------------------------------------------------------
# Heartbeat Thread
# -------------------------------------------------------

def send_heartbeat(producer_id, producer_name, location_id, kafka_broker, interval_seconds=30):
    topic = "producer_heartbeat"
    producer = None

    try:
        producer = KafkaProducer(
            bootstrap_servers=kafka_broker,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

        while running:
            message = {
                'producer_id': producer_id,
                'producer_name': producer_name,
                'location_id': location_id,
                'timestamp': datetime.now().isoformat()
            }

            try:
                producer.send(topic, message)
                producer.flush(timeout=10)
                source_logger.debug(f"Sent heartbeat: {message}")
            except Exception as e:
                source_logger.error(f"Failed to send heartbeat: {e}")

            for _ in range(interval_seconds):
                if not running:
                    break
                time.sleep(1)

    except Exception as e:
        source_logger.error(f"Heartbeat failed: {e}")
    finally:
        if producer:
            try:
                producer.close()
            except Exception:
                pass


# -------------------------------------------------------
# DB Fetch Function
# -------------------------------------------------------

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

def delete_old_logs():
    source_logger.info("Log deletion thread started (TEST MODE)")

    while running:
        try:
            cutoff_time = datetime.now() - timedelta(hours=DELETE_LOGS_OLDER_THAN_HOURS)

            for filename in os.listdir(logs_dir):
                file_path = os.path.join(logs_dir, filename)

                # skip non-files
                if not os.path.isfile(file_path):
                    continue

                # do NOT delete active log file
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
            source_logger.error(f"Log cleanup failed: {e}")

        # sleep safely
        for _ in range(LOG_DELETE_INTERVAL_SECONDS):
            if not running:
                return
            time.sleep(1)


# -------------------------------------------------------
# Windows Service Wrapper
# -------------------------------------------------------

class ProducerService(win32serviceutil.ServiceFramework):
    _svc_name_ = 'producer'
    _svc_display_name_ = 'Producer Service'

    def __init__(self, args):
        super().__init__(args)
        self.hWaitStop = win32event.CreateEvent(None, 0, 0, None)
        self.running = True

    def SvcStop(self):
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        win32event.SetEvent(self.hWaitStop)
        self.running = False
        source_logger.info("Service is stopping...")

    def SvcDoRun(self):
        self.ReportServiceStatus(win32service.SERVICE_RUNNING)
        try:
            main()
            win32event.WaitForSingleObject(self.hWaitStop, win32event.INFINITE)
        except Exception as e:
            self.SvcStop()
            servicemanager.LogErrorMsg(str(e))


# -------------------------------------------------------
# Main Application Loop
# -------------------------------------------------------

def main():
    source_logger.info("Starting main producer")

    # Start flush thread
    threading.Thread(target=_periodic_log_flush, daemon=True).start()

    # Start heartbeat
    threading.Thread(
        target=send_heartbeat,
        args=(producer_id, producer_name, location_id, kafka_broker),
        daemon=True
    ).start()

    # 🔥 START LOG DELETION THREAD (TEST)
    threading.Thread(
        target=delete_old_logs,
        daemon=True
    ).start()

    try:
        while running:
            for table_name, cfg in tables.items():
                fetch_and_send_data(
                    table_name,
                    check_dbstatus=cfg.get("check_dbstatus", False),
                    exclude_columns=cfg.get("exclude_columns", [])
                )

            source_logger.debug("Waiting 5 sec for next cycle...")
            time.sleep(5)

    except Exception as e:
        source_logger.error(f"Fatal error in main loop: {e}")
    finally:
        source_logger.info("Producer shutting down...")
        time.sleep(1)


if __name__ == "__main__":
    main()
