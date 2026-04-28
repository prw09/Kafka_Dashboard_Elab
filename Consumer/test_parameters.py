import os
import json
import time
import signal
import logging
from datetime import datetime

import pyodbc
from dotenv import load_dotenv
from kafka import KafkaConsumer
from kafka.errors import KafkaError, NoBrokersAvailable, CommitFailedError
from logging.handlers import TimedRotatingFileHandler

# -----------------------------
# GLOBAL FLAG
# -----------------------------
running = True

# -----------------------------
# LOAD ENV
# -----------------------------
env_file = os.getenv("ENV_FILE")

if not env_file or not os.path.exists(env_file):
    raise Exception("ENV file not set or not found")

load_dotenv(env_file)

# -----------------------------
# CONFIG
# -----------------------------
KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
GROUP_ID = os.getenv("TP_CONSUMER_GROUP", "test_parameter_group")

TABLE_NAME = "dbo.Test_Parameters"

BATCH_SIZE = 500  # 🔥 tune: 200–1000

columns = [
    'ResultID','OrderID','PatientMasterID','MacDataGuid','ParameterCode','TestCode',
    'Result','ResultType','DbStatus','CreatedDate','ResultReceivedDate','ResultUpdateDate',
    'ModifiedDate','MachineFID','LocationID','IsSync','InstrumentId'
]

# -----------------------------
# LOGGING
# -----------------------------
# -----------------------------
# LOGGING SETUP (ORIGINAL STYLE)
# -----------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(BASE_DIR, "logs_tp")

os.makedirs(LOG_DIR, exist_ok=True)

LOG_FORMAT = "%(asctime)s | %(levelname)s | %(process)d | %(message)s"

INSTANCE_NAME = os.getenv("TP_INSTANCE_NAME", f"tp_consumer_{os.getpid()}")
LOG_RETENTION_DAYS = int(os.getenv("LOG_RETENTION_DAYS", "3"))

logger = logging.getLogger(INSTANCE_NAME)
logger.setLevel(logging.INFO)
logger.handlers.clear()

# 🔴 FILE HANDLER (HOURLY ROTATION)
file_handler = TimedRotatingFileHandler(
    filename=os.path.join(LOG_DIR, f"{INSTANCE_NAME}.log"),
    when="H",
    interval=1,
    backupCount=24 * LOG_RETENTION_DAYS,
    encoding="utf-8"
)

file_handler.suffix = "%Y-%m-%d_%H.log"
file_handler.setFormatter(logging.Formatter(LOG_FORMAT))
file_handler.setLevel(logging.INFO)

# 🟢 CONSOLE HANDLER
console = logging.StreamHandler()
console.setFormatter(logging.Formatter(LOG_FORMAT))
console.setLevel(logging.INFO)

logger.addHandler(file_handler)
logger.addHandler(console)

logger.propagate = False

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
    for key in ["CreatedDate","ModifiedDate","ResultReceivedDate","ResultUpdateDate"]:
        if key in record and isinstance(record[key], str):
            try:
                record[key] = datetime.fromisoformat(record[key])
            except:
                record[key] = None
    return record

# -----------------------------
# DB CONNECTION
# -----------------------------
def get_connection():
    return pyodbc.connect(
        f"DRIVER={{{os.getenv('DB_DRIVER')}}};"
        f"SERVER={os.getenv('DB_SERVER')};"
        f"DATABASE={os.getenv('DB_NAME')};"
        f"UID={os.getenv('DB_USER')};"
        f"PWD={os.getenv('DB_PASSWORD')};"
        "Encrypt=yes;"
        "TrustServerCertificate=yes;",
        autocommit=False
    )

def safe_process_batch(cursor, conn, records):

    try:
        process_batch(cursor, records)
        conn.commit()
        return True

    except Exception as e:
        logger.error(f"❌ BATCH FAILED | size={len(records)} | error={e}")
        conn.rollback()

        # If only 1 record → log it directly
        if len(records) == 1:
            logger.error(f"🚨 BAD RECORD: {records[0]}")
            return False

        # Split batch into halves
        mid = len(records) // 2

        left = records[:mid]
        right = records[mid:]

        safe_process_batch(cursor, conn, left)
        safe_process_batch(cursor, conn, right)


        return False


# -----------------------------
# BATCH UPSERT WITH DB-STATUS
# -----------------------------

def process_batch(cursor, records):

    # STATUS INSERT AND UPDATE ISSUE FIXED
    merge_query = f"""
    MERGE {TABLE_NAME} AS target
    USING (VALUES ({",".join(["?"] * len(columns))})) AS source ({",".join(columns)})
    ON target.ResultID = source.ResultID
       AND target.LocationID = source.LocationID

    WHEN MATCHED
    THEN UPDATE SET
        {",".join([f"{col}=source.{col}" for col in columns if col not in ("ResultID", "LocationID")])}

    WHEN NOT MATCHED
    THEN INSERT ({",".join(columns)})
         VALUES ({",".join([f"source.{col}" for col in columns])});
    """

    values = []

    for record in records:
        record = parse_dates(record)

        raw_status = record.get("DbStatus")

        try:
            db_status = int(raw_status) if raw_status is not None else None
        except (ValueError, TypeError):
            db_status = None

        record["DbStatus"] = db_status

        logger.info(
            f"ResultID={record.get('ResultID')} | "
            f"LocationID={record.get('LocationID')} | "
            f"RawDbStatus={raw_status} ({type(raw_status)}) | "
            f"NormalizedDbStatus={db_status} ({type(db_status)})"
        )

        values.append([record.get(col) for col in columns])

    if not values:
        logger.info("No valid records to process in this batch")
        return

    cursor.fast_executemany = True
    cursor.executemany(merge_query, values)

    logger.info(f"Batch sent to DB successfully | valid_records={len(values)}")

# -----------------------------
# CONSUMER
# -----------------------------
def consume():

    conn = get_connection()
    cursor = conn.cursor()

    consumer = KafkaConsumer(
        TABLE_NAME,
        bootstrap_servers=KAFKA_SERVERS,
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        enable_auto_commit=False,
        group_id=GROUP_ID,

        # PERFORMANCE
        max_poll_records=1000,
        fetch_max_bytes=52428800,
        fetch_max_wait_ms=500,

        session_timeout_ms=30000,
        heartbeat_interval_ms=10000,
        auto_offset_reset="latest"

    )

    logger.info("🚀 TP Consumer started")

    while running:

        batch = consumer.poll(timeout_ms=5000)

        if not batch:
            continue

        all_records = []

        try:
            for _, messages in batch.items():

                for msg in messages:
                    all_records.append(msg.value)

                    if len(all_records) >= BATCH_SIZE:
                        process_batch(cursor, all_records)
                        conn.commit()
                        consumer.commit()

                        logger.info(f"✅ Batch processed: {len(all_records)}")
                        all_records.clear()

            # leftover batch
            if all_records:
                #process_batch(cursor, all_records)
                #conn.commit()
                #consumer.commit()
                success = safe_process_batch(cursor, conn, all_records)

                if success:
                    consumer.commit()

                logger.info(f"✅ Final batch processed: {len(all_records)}")
                all_records.clear()

        except Exception as e:
            logger.error(f"❌ BATCH ERROR | {e}")
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