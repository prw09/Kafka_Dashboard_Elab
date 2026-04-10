import os
import json
import time
import signal
import logging
from datetime import datetime

import pyodbc
from dotenv import load_dotenv
from kafka import KafkaConsumer, TopicPartition, OffsetAndMetadata
from kafka.errors import KafkaError, NoBrokersAvailable, CommitFailedError
from logging.handlers import TimedRotatingFileHandler

# -----------------------------
# GLOBAL FLAG
# -----------------------------
running = True

# env config
env_file = os.getenv("ENV_FILE")

if not env_file:
    raise Exception("ENV_FILE not set. Please configure in NSSM")

if not os.path.exists(env_file):
    raise Exception(f"ENV file not found: {env_file}")

load_dotenv(env_file)

# -----------------------------
# ENV CONFIG
# -----------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(BASE_DIR, "logs_tp")

KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
GROUP_ID = os.getenv("TP_CONSUMER_GROUP", "test_parameter_group")

# LOG CONFIG FROM ENV
LOG_RETENTION_DAYS = int(os.getenv("LOG_RETENTION_DAYS", "3"))
INSTANCE_NAME = os.getenv("TP_INSTANCE_NAME", f"tp_consumer_{os.getpid()}")

os.makedirs(LOG_DIR, exist_ok=True)

# -----------------------------
# LOGGING SETUP
# -----------------------------
LOG_FORMAT = "%(asctime)s | %(levelname)s | %(process)d | %(message)s"

logger = logging.getLogger(INSTANCE_NAME)
logger.setLevel(logging.INFO)
logger.handlers.clear()

#  ERROR FILE HANDLER (HOURLY ROTATION)
file_handler = TimedRotatingFileHandler(
    filename=os.path.join(LOG_DIR, f"{INSTANCE_NAME}.log"),
    when="H",
    interval=1,
    backupCount=24 * LOG_RETENTION_DAYS,  #  retention control
    encoding="utf-8"
)

#  FILE NAME FORMAT: date + hour
file_handler.suffix = "%Y-%m-%d_%H.log"

file_handler.setFormatter(logging.Formatter(LOG_FORMAT))
file_handler.setLevel(logging.ERROR)
logger.addHandler(file_handler)

# Console (optional)
console = logging.StreamHandler()
console.setFormatter(logging.Formatter(LOG_FORMAT))
console.setLevel(logging.INFO)
logger.addHandler(console)

logger.propagate = False

# -----------------------------
# TABLE CONFIG
# -----------------------------
TABLE_NAME = "dbo.Test_Parameters"

columns = ['ResultID', 'OrderID', 'PatientMasterID', 'MacDataGuid', 'ParameterCode', 'TestCode',
                            'Result', 'ResultType', 'DbStatus', 'CreatedDate', 'ResultReceivedDate', 'ResultUpdateDate',
                            'ModifiedDate', 'MachineFID', 'LocationID', 'IsSync', 'InstrumentId']

# -----------------------------
# SIGNAL HANDLING
# -----------------------------
def shutdown(signum, frame):
    global running
    logger.info("Shutdown signal received...")
    running = False

signal.signal(signal.SIGINT, shutdown)
signal.signal(signal.SIGTERM, shutdown)

# -----------------------------
# DATE PARSER
# -----------------------------
def parse_dates(record):

    date_fields = ["CreatedDate","ModifiedDate","ResultReceivedDate","ResultUpdateDate"]

    for key in date_fields:
        if key in record and isinstance(record[key], str):
            try:
                record[key] = datetime.fromisoformat(record[key])
            except:
                record[key] = None

    return record

# -----------------------------
# PROCESS MESSAGE (WITH DBSTATUS)
# -----------------------------
def process_message(cursor, record):

    primary_key = columns[0]
    pk_val = record.get(primary_key)
    location_id = record.get("LocationID")

    if pk_val is None:
        logger.error("Missing primary key")
        return False

    if location_id is None:
        logger.error("Missing LocationID")
        return False

    try:
        record = parse_dates(record)

        #  DBSTATUS CHECK
        if "DbStatus" in columns:
            cursor.execute(
                f"""
                SELECT DbStatus FROM {TABLE_NAME}
                WHERE {primary_key}=? AND LocationID=?
                """,
                (pk_val, location_id)
            )

            existing = cursor.fetchone()

            if existing:
                current = existing[0] or 0
                incoming = record.get("DbStatus", 0) or 0

                if incoming < current:
                    return True  # skip safely

        # CHECK EXISTENCE
        cursor.execute(
            f"""
            SELECT 1 FROM {TABLE_NAME}
            WHERE {primary_key}=? AND LocationID=?
            """,
            (pk_val, location_id)
        )

        exists = cursor.fetchone()

        if exists:
            update_cols = [c for c in columns if c != primary_key]
            query = f"""
                UPDATE {TABLE_NAME}
                SET {",".join([f"{c}=?" for c in update_cols])}
                WHERE {primary_key}=? AND LocationID=?
            """

            params = [record.get(c) for c in update_cols] + [pk_val, location_id]
            cursor.execute(query, params)

        else:
            query = f"""
                INSERT INTO {TABLE_NAME} ({",".join(columns)})
                VALUES ({",".join(["?"] * len(columns))})
            """
            values = [record.get(c) for c in columns]
            cursor.execute(query, values)

        return True

    except Exception as e:
        logger.error(f"PROCESS ERROR | PK={pk_val} | error={e}")
        return False

# -----------------------------
# CONSUMER
# -----------------------------
def consume():

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

    consumer = KafkaConsumer(
        TABLE_NAME,
        bootstrap_servers=KAFKA_SERVERS,
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        enable_auto_commit=False,
        group_id=GROUP_ID,

        # PERFORMANCE
        max_poll_records=500,
        fetch_max_bytes=52428800,
        fetch_max_wait_ms=500,

        session_timeout_ms=30000,
        heartbeat_interval_ms=10000,
        auto_offset_reset="latest"
    )

    logger.info("TP Consumer started")

    while running:

        batch = consumer.poll(timeout_ms=5000)

        if not batch:
            continue

        try:
            offsets = {}
            success_all = True

            for tp, messages in batch.items():

                last_offset = None

                for msg in messages:
                    ok = process_message(cursor, msg.value)

                    if not ok:
                        success_all = False
                        break

                    last_offset = msg.offset

                if not success_all:
                    break

                if last_offset is not None:
                    offsets[tp] = OffsetAndMetadata(last_offset + 1, None)

            if success_all:
                conn.commit()

                if offsets:
                    consumer.commit(offsets=offsets)

            else:
                conn.rollback()

        except Exception as e:
            logger.error(f"BATCH ERROR | {e}")
            conn.rollback()

    consumer.close()
    conn.close()

# -----------------------------
# MAIN WITH RETRY
# -----------------------------
if __name__ == "__main__":

    retry = 0

    while running:
        try:
            consume()
            retry = 0

        except (NoBrokersAvailable, CommitFailedError, pyodbc.OperationalError) as e:
            delay = min(10 * (2 ** retry), 300)
            retry += 1
            logger.error(f"CONNECTION ERROR | {e}")

        except KafkaError as e:
            delay = 15
            logger.error(f"KAFKA ERROR | {e}")

        except Exception as e:
            delay = 30
            retry += 1
            logger.error(f"UNEXPECTED ERROR | {e}")

        if running:
            time.sleep(delay)
