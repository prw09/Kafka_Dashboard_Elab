import os
import json
import time
import signal
import shutil
import threading
import logging
from datetime import datetime, timedelta
from logging.handlers import TimedRotatingFileHandler

import pyodbc
from dotenv import load_dotenv
from kafka import KafkaConsumer, TopicPartition, OffsetAndMetadata
from kafka.errors import KafkaError, NoBrokersAvailable, CommitFailedError

# -----------------------------
# Global control flag
# -----------------------------
running = True

# -----------------------------
# Base paths
# -----------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))          # ...\kafkaConsumer\consumer
PROJECT_ROOT = os.path.dirname(BASE_DIR)                       # ...\kafkaConsumer
ENV_PATH = os.path.join(PROJECT_ROOT, ".env")
LOG_DIR = os.path.join(BASE_DIR, "logs")
ARCHIVE_DIR = os.path.join(LOG_DIR, "archive")

# -----------------------------
# Load environment variables
# -----------------------------
load_dotenv(ENV_PATH)

INSTANCE_NAME = os.getenv("CONSUMER_INSTANCE_NAME", "lab_data_consumer")

os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(ARCHIVE_DIR, exist_ok=True)

# -----------------------------
# Logging Configuration
# Changes:
# 1. Rotate every hour
# 2. Keep current logs in logs/ for 1 day
# 3. Move older logs to logs/archive/
# 4. Delete archived logs after 6 days
# 5. Reduce noisy logs by using DEBUG for full payload
# -----------------------------
LOG_FORMAT = (
    "%(asctime)s | %(levelname)s | %(process)d | %(threadName)s | "
    "%(filename)s:%(lineno)d | %(message)s"
)
formatter = logging.Formatter(LOG_FORMAT)

# Main log: rotate hourly, keep last 24 rotated files in main folder
file_handler = TimedRotatingFileHandler(
    filename=os.path.join(LOG_DIR, f"{INSTANCE_NAME}.log"),
    when="H",
    interval=1,
    backupCount=24,
    encoding="utf-8"
)
file_handler.suffix = "%Y-%m-%d_%H.log"
file_handler.setFormatter(formatter)
file_handler.setLevel(logging.INFO)

# Error-only log: rotate hourly, keep last 24 rotated files in main folder
error_handler = TimedRotatingFileHandler(
    filename=os.path.join(LOG_DIR, f"{INSTANCE_NAME}_error.log"),
    when="H",
    interval=1,
    backupCount=24,
    encoding="utf-8"
)
error_handler.suffix = "%Y-%m-%d_%H.log"
error_handler.setFormatter(formatter)
error_handler.setLevel(logging.ERROR)

# Console handler is optional in production
ENABLE_CONSOLE_LOG = os.getenv("ENABLE_CONSOLE_LOG", "false").lower() == "true"

logger = logging.getLogger("KafkaConsumerService")
logger.setLevel(logging.INFO)
logger.handlers.clear()
logger.addHandler(file_handler)
logger.addHandler(error_handler)

if ENABLE_CONSOLE_LOG:
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.INFO)
    logger.addHandler(console_handler)

logger.propagate = False

# -----------------------------
# Tables handled by OLD consumer
# IMPORTANT CHANGE:
# dbo.Test_Parameters is REMOVED from old consumer
# because it is handled by the new dedicated TP consumer
# -----------------------------
tables_columns = {
    "dbo.Patient_Details": [
        "PatientMasterID", "PatientID", "PatientName", "DOB", "Gender", "PatLocationID",
        "PatLocationName", "CreateDate", "ModifiedDate", "LocationID", "IsSync"
    ],
    "dbo.Orders": [
        "OrderID", "PatientMasterID", "BarcodeNo", "BarcodeNoID", "TestCode", "Sampletype",
        "SampleTypeID", "Samplecollectiontime", "DbStatus", "CreatedDate", "ModifiedDate",
        "LocationID", "IsSync"
    ],

    "dbo.ProductAuth_Table": [
        "AuthID", "AuthKey", "CreateDate", "ModifiedDate", "ExpTime", "MacID", "MachineName",
        "LocationID", "IsSync"
    ],
    "dbo.Users": [
        "id", "firstName", "lastName", "email", "phoneNumber", "location", "role", "userName",
        "confirmUserName", "password", "confirmPassword", "createdDate", "modifiedDate",
        "IsDeleted", "IsActive", "LocationID", "IsSync"
    ],
    "dbo.ExtTestCodeConfiguration": [
        "ID", "LISParamId", "LISParamName", "IsDeleted", "CreateDate", "ModifiedDate",
        "LocationID", "IsSync"
    ],
    "dbo.UtilityException": [
        "ID", "MessageString", "ErrorCode", "Timestamp", "MachineFID", "BarcodeNo",
        "ModifiedDate", "LocationID", "IsSync"
    ],
    "dbo.LocationMaster": [
        "LocationID", "LocationName", "CreateDate", "ModifiedDate", "CenterId", "IsSync"
    ],
    "dbo.KafkaBrokerStatus": [
        "ID", "LocationID", "logType", "issync", "created_at", "updated_at", "IsRunning"
    ],
    "dbo.AppVersionLog": [
        "Id", "InstallationVersionNumber", "InstallationSystemName", "UserName", "InstrumentName",
        "LocationName", "CenterId", "LogDate", "BuildVersion", "BuildDate", "IsSync"
    ]
}

# -----------------------------
# Log management thread
# Moves logs older than 1 day to archive
# Deletes archived logs older than 6 days
# -----------------------------
def manage_logs():
    while running:
        try:
            now = datetime.now()

            # Move old logs from main log folder to archive after 1 day
            for fname in os.listdir(LOG_DIR):
                if fname == "archive":
                    continue

                if not (
                        fname.startswith(f"{INSTANCE_NAME}.log")
                        or fname.startswith(f"{INSTANCE_NAME}_error.log")
                ):
                    continue

                fpath = os.path.join(LOG_DIR, fname)
                if not os.path.isfile(fpath):
                    continue

                file_time = datetime.fromtimestamp(os.path.getmtime(fpath))
                if now - file_time > timedelta(days=1):
                    archive_path = os.path.join(ARCHIVE_DIR, fname)
                    if not os.path.exists(archive_path):
                        shutil.move(fpath, archive_path)

            # Delete archived logs older than 6 days
            for fname in os.listdir(ARCHIVE_DIR):
                fpath = os.path.join(ARCHIVE_DIR, fname)
                if not os.path.isfile(fpath):
                    continue

                file_time = datetime.fromtimestamp(os.path.getmtime(fpath))
                if now - file_time > timedelta(days=6):
                    os.remove(fpath)

        except Exception as e:
            logger.error(f"LOG MANAGEMENT ERROR | error={e}")

        # Run once per hour
        time.sleep(3600)

# -----------------------------
# Signal handlers
# Changed:
# graceful shutdown using running flag
# no hard os._exit()
# -----------------------------
def handle_interrupt(signum, frame):
    global running
    logger.info("Keyboard interrupt detected. Shutting down.")
    running = False

def handle_shutdown(signum, frame):
    global running
    logger.info("System is shutting down.")
    running = False

# -----------------------------
# JSON logging helper
# Reduced noise:
# only log useful info, no TP-specific warnings here
# because TP is now handled by separate consumer
# -----------------------------
def log_json_issues(record, table_name):
    if table_name == "dbo.Orders":
        logger.debug(
            f"JSON Fields | Table={table_name} | "
            f"OrderID={record.get('OrderID')} | DbStatus={record.get('DbStatus')} | "
            f"LocationID={record.get('LocationID')}"
        )
    elif table_name == "dbo.Patient_Details":
        logger.debug(
            f"JSON Fields | Table={table_name} | "
            f"PatientMasterID={record.get('PatientMasterID')} | "
            f"LocationID={record.get('LocationID')}"
        )
    elif table_name == "dbo.UtilityException":
        logger.debug(
            f"JSON Fields | Table={table_name} | "
            f"ID={record.get('ID')} | ErrorCode={record.get('ErrorCode')} | "
            f"LocationID={record.get('LocationID')}"
        )

# -----------------------------
# Process one Kafka message
# Important changes:
# 1. TP removed from this old consumer
# 2. LocationID validation fixed
# 3. Skip logic changed from <= to <
#    so same-status rows can still update
# -----------------------------
def process_message(cursor, message):
    record = message.value
    table_name = message.topic

    logger.info(f"Processing message from {table_name}")
    log_json_issues(record, table_name)

    columns = tables_columns.get(table_name)
    if not columns:
        logger.error(f"Unknown table: {table_name}")
        return False

    # Parse date strings into datetime objects
    date_fields = [
        "CreatedDate", "Createdate", "createdDate", "modifiedDate",
        "ModifiedDate", "CreateDate", "Timestamp", "Samplecollectiontime",
        "created_at", "updated_at", "DOB", "ExpTime", "LogDate", "BuildDate"
    ]

    for key in date_fields:
        if key in record and isinstance(record[key], str):
            try:
                record[key] = datetime.fromisoformat(record[key])
            except (ValueError, TypeError):
                record[key] = None

    primary_key = columns[0]
    primary_key_value = record.get(primary_key)
    location_id = record.get("LocationID")

    if primary_key_value is None:
        logger.error(f"Missing primary key '{primary_key}' in {table_name} record")
        return False

    # IMPORTANT CHANGE:
    # use is None instead of if not location_id
    if location_id is None:
        logger.error(f"Missing LocationID in {table_name} record")
        return False

    try:
        # Only skip if incoming DbStatus is LOWER than current
        # Same-status rows are allowed to continue to update path
        if "DbStatus" in columns:
            cursor.execute(
                f"SELECT DbStatus FROM {table_name} WHERE {primary_key} = ? AND LocationID = ?",
                (primary_key_value, location_id)
            )
            existing = cursor.fetchone()

            if existing:
                current_db_status = existing[0] if existing[0] is not None else 0
                incoming_db_status = record.get("DbStatus", 0) or 0

                if incoming_db_status < current_db_status:
                    logger.info(
                        f"Skipping older DbStatus update | table={table_name} | "
                        f"{primary_key}={primary_key_value} | "
                        f"incoming={incoming_db_status} | existing={current_db_status}"
                    )
                    return True

        values = [record.get(col, None) for col in columns]

        # Check if row exists
        cursor.execute(
            f"SELECT 1 FROM {table_name} WHERE {primary_key} = ? AND LocationID = ?",
            (primary_key_value, location_id)
        )
        exists = cursor.fetchone()

        if exists:
            update_columns = [col for col in columns if col != primary_key]
            update_assignments = ", ".join([f"{col} = ?" for col in update_columns])
            query = (
                f"UPDATE {table_name} "
                f"SET {update_assignments} "
                f"WHERE {primary_key} = ? AND LocationID = ?"
            )
            params = [record.get(col, None) for col in update_columns] + [primary_key_value, location_id]
            cursor.execute(query, params)

            logger.info(
                f"UPDATED | table={table_name} | {primary_key}={primary_key_value} | LocationID={location_id}"
            )
        else:
            query = (
                f"INSERT INTO {table_name} ({', '.join(columns)}) "
                f"VALUES ({', '.join(['?'] * len(columns))})"
            )
            cursor.execute(query, values)

            logger.info(
                f"INSERTED | table={table_name} | {primary_key}={primary_key_value} | LocationID={location_id}"
            )

        return True

    except pyodbc.Error as e:
        logger.error(
            f"DATABASE ERROR | table={table_name} | {primary_key}={primary_key_value} | "
            f"LocationID={location_id} | error={e}"
        )
        raise
    except Exception as e:
        logger.error(
            f"PROCESSING ERROR | table={table_name} | {primary_key}={primary_key_value} | "
            f"LocationID={location_id} | error={e}"
        )
        return False

# -----------------------------
# Consume messages
# Important changes:
# 1. DB commit happens BEFORE Kafka offset commit
# 2. Partition-wise offset commit
# 3. No full JSON payload logging at INFO
# -----------------------------
def consume_messages(consumer_group_id):
    consumer = None
    conn = None

    try:
        logger.info("Opening central DB connection...")

        conn = pyodbc.connect(
            f"DRIVER={{{os.getenv('DB_DRIVER')}}};"
            f"SERVER={os.getenv('DB_SERVER')};"
            f"DATABASE={os.getenv('DB_NAME')};"
            f"UID={os.getenv('DB_USER')};"
            f"PWD={os.getenv('DB_PASSWORD')};"
            "Encrypt=yes;"
            "TrustServerCertificate=yes;"
        )
        cursor = conn.cursor()

        kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS")

        consumer = KafkaConsumer(
            *tables_columns.keys(),
            bootstrap_servers=kafka_bootstrap_servers,
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
            enable_auto_commit=False,
            auto_offset_reset="latest",
            group_id=consumer_group_id,
            session_timeout_ms=30000,
            heartbeat_interval_ms=10000,
            max_poll_records=100,
            request_timeout_ms=40000
        )

        logger.info("Old consumer initialized. Starting message processing...")

        while running:
            try:
                batch = consumer.poll(timeout_ms=10000, max_records=100)
            except KafkaError as e:
                logger.error(f"POLL ERROR | error={e}")
                raise
            except Exception as e:
                logger.error(f"UNEXPECTED POLL ERROR | error={e}")
                raise

            if not batch:
                continue

            try:
                offsets_to_commit = {}
                all_success = True

                for topic_partition, messages in batch.items():
                    last_successful_offset = None

                    for message in messages:
                        record = message.value
                        logger.debug(
                            "Received Message: %s",
                            json.dumps(record, indent=2, default=str)
                        )

                        success = process_message(cursor, message)

                        if not success:
                            logger.error(f"Failed to process message: {record}")
                            all_success = False
                            break

                        last_successful_offset = message.offset

                    if not all_success:
                        break

                    if last_successful_offset is not None:
                        tp = TopicPartition(topic_partition.topic, topic_partition.partition)
                        offsets_to_commit[tp] = OffsetAndMetadata(last_successful_offset + 1, None)

                # IMPORTANT CHANGE:
                # commit DB first
                if all_success:
                    conn.commit()
                    logger.info("DB COMMIT SUCCESS")

                    if offsets_to_commit:
                        consumer.commit(offsets=offsets_to_commit)
                        logger.info(
                            f"OFFSET COMMIT SUCCESS | partitions={len(offsets_to_commit)}"
                        )
                else:
                    conn.rollback()
                    logger.error("BATCH FAILED | DB rolled back | offsets not committed")

            except (pyodbc.OperationalError, KafkaError) as e:
                logger.error(f"CRITICAL ERROR DURING PROCESSING | error={e}")
                conn.rollback()
                raise
            except Exception as e:
                logger.error(f"UNEXPECTED ERROR DURING PROCESSING | error={e}")
                conn.rollback()
                continue

    finally:
        logger.info("Cleaning up resources...")
        try:
            if consumer:
                consumer.close()
        except Exception:
            pass

        try:
            if conn:
                conn.close()
        except Exception:
            pass


# -----------------------------
# Main
# -----------------------------
if __name__ == "__main__":
    signal.signal(signal.SIGINT, handle_interrupt)
    signal.signal(signal.SIGTERM, handle_shutdown)

    # This old consumer continues to use the old group
    consumer_group_id = os.getenv("KAFKA_CONSUMER_GROUP")
    logger.info(f"Using old consumer group: {consumer_group_id}")

    # Start log management thread
    log_manager_thread = threading.Thread(
        target=manage_logs,
        name="LogManager",
        daemon=True
    )
    log_manager_thread.start()

    retry_count = 0
    max_retry_delay = 300  # 5 minutes

    while running:
        try:
            logger.info(f"Starting old consumer (attempt {retry_count + 1})...")
            consume_messages(consumer_group_id)
            retry_count = 0

        except (NoBrokersAvailable, CommitFailedError, pyodbc.OperationalError) as e:
            logger.error(f"CONNECTION ERROR | error={e}")
            delay = min(10 * (2 ** retry_count), max_retry_delay)
            retry_count += 1

        except KafkaError as e:
            logger.error(f"KAFKA ERROR | error={e}")
            delay = 15
            retry_count = 0

        except Exception as e:
            logger.error(f"UNEXPECTED TOP-LEVEL ERROR | error={e}")
            delay = 30
            retry_count += 1

        if running:
            logger.info(f"Restarting in {delay} seconds...")
            time.sleep(delay)

    logger.info("Old consumer stopped gracefully.")