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
GROUP_ID      = os.getenv("TP_CONSUMER_GROUP", "test_parameter_group")
TABLE_NAME    = "dbo.Test_Parameters"
BATCH_SIZE    = 500
MAX_RETRIES   = 3

columns = [
    'ResultID','OrderID','PatientMasterID','MacDataGuid','ParameterCode','TestCode',
    'Result','ResultType','DbStatus','CreatedDate','ResultReceivedDate','ResultUpdateDate',
    'ModifiedDate','MachineFID','LocationID','IsSync','InstrumentId'
]

# -----------------------------
# JSON LOGGER
# -----------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_DIR  = os.path.join(BASE_DIR, "logs_tp")
os.makedirs(LOG_DIR, exist_ok=True)

INSTANCE_NAME      = os.getenv("TP_INSTANCE_NAME", f"tp_consumer_{os.getpid()}")
LOG_RETENTION_DAYS = int(os.getenv("LOG_RETENTION_DAYS", "3"))

class JsonFormatter(logging.Formatter):
    def format(self, record):
        base = {
            "ts":       self.formatTime(record, "%Y-%m-%dT%H:%M:%S"),
            "level":    record.levelname,
            "pid":      record.process,
            "instance": INSTANCE_NAME,
            "msg":      record.getMessage(),
        }
        for key, val in record.__dict__.items():
            if key.startswith("x_"):
                base[key[2:]] = val
        if record.exc_info:
            base["exc"] = self.formatException(record.exc_info)
        return json.dumps(base, default=str)

logger = logging.getLogger(INSTANCE_NAME)
logger.setLevel(logging.INFO)
logger.handlers.clear()

file_handler = TimedRotatingFileHandler(
    filename=os.path.join(LOG_DIR, f"{INSTANCE_NAME}.jsonl"),
    when="H", interval=1,
    backupCount=24 * LOG_RETENTION_DAYS,
    encoding="utf-8"
)
file_handler.suffix = "%Y-%m-%d_%H.jsonl"
file_handler.setFormatter(JsonFormatter())
file_handler.setLevel(logging.INFO)

console = logging.StreamHandler()
console.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(process)d | %(message)s"))
console.setLevel(logging.INFO)

logger.addHandler(file_handler)
logger.addHandler(console)
logger.propagate = False

# -----------------------------
# STRUCTURED LOG HELPER
# -----------------------------
def log(level, msg, **fields):
    extra = {f"x_{k}": v for k, v in fields.items()}
    getattr(logger, level)(msg, extra=extra)

# -----------------------------
# SIGNAL HANDLING
# -----------------------------
def shutdown(signum, frame):
    global running
    log("info", "Shutdown signal received", signal=signum)
    running = False

signal.signal(signal.SIGINT,  shutdown)
signal.signal(signal.SIGTERM, shutdown)

# -----------------------------
# DATE PARSER
# -----------------------------
def parse_dates(record):
    for key in ["CreatedDate", "ModifiedDate", "ResultReceivedDate", "ResultUpdateDate"]:
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
        "Encrypt=yes;TrustServerCertificate=yes;",
        autocommit=False
    )

def get_connection_with_retry(max_wait=300):
    retry = 0
    while running:
        try:
            conn = get_connection()
            log("info", "✅ DB connected", attempt=retry + 1)
            return conn
        except pyodbc.OperationalError as e:
            delay = min(10 * (2 ** retry), max_wait)
            retry += 1
            log("error", "❌ DB down — retrying",
                error=str(e),
                attempt=retry,
                next_retry_sec=delay
            )
            time.sleep(delay)

def ensure_connection(conn):
    try:
        conn.cursor().execute("SELECT 1")
        return conn
    except Exception:
        log("warning", "⚠️ DB connection lost — reconnecting...")
        try:
            conn.close()
        except:
            pass
        return get_connection_with_retry()

# -----------------------------
# BATCH UPSERT
# -----------------------------
def process_batch(cursor, records):
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
        record     = parse_dates(record)
        raw_status = record.get("DbStatus")

        try:
            db_status = int(raw_status) if raw_status is not None else None
        except (ValueError, TypeError):
            db_status = None

        record["DbStatus"] = db_status
        values.append([record.get(col) for col in columns])

    if not values:
        log("info", "No valid records in batch")
        return

    cursor.fast_executemany = True
    cursor.executemany(merge_query, values)

# -----------------------------
# SAFE BATCH WITH DEADLOCK RETRY + DB RECONNECT
# -----------------------------
def safe_process_batch(cursor, conn, records, retry_count=0):
    t_start = time.time()

    try:
        process_batch(cursor, records)
        conn.commit()

        elapsed = round(time.time() - t_start, 3)

        status_counts = {}
        for r in records:
            s = str(r.get("DbStatus", "?"))
            status_counts[s] = status_counts.get(s, 0) + 1

        log("info", "✅ BATCH OK",
            size=len(records),
            elapsed_sec=elapsed,
            status_counts=status_counts,
            retry=retry_count,
            result_ids=[r.get("ResultID") for r in records]
        )
        return True, conn

    except Exception as e:
        error_str   = str(e)
        is_deadlock = '40001' in error_str
        is_db_down  = any(code in error_str for code in [
            '08001', '08S01', 'HYT00', 'IM002', '08003',
            'Communication link failure',
            'TCP Provider',
            'named pipe',
        ])

        try:
            conn.rollback()
        except:
            pass

        # 🔴 DB DOWN → reconnect and retry same batch
        if is_db_down:
            log("error", "🔴 DB DOWN — waiting for recovery",
                size=len(records),
                error=error_str
            )
            conn   = get_connection_with_retry()
            cursor = conn.cursor()
            log("info", "🟢 DB recovered — retrying batch", size=len(records))
            return safe_process_batch(cursor, conn, records, retry_count)

        # ⚠️ DEADLOCK → retry with backoff
        if is_deadlock and retry_count < MAX_RETRIES:
            delay = 2 ** retry_count
            log("warning", "⚠️ DEADLOCK — retrying",
                size=len(records),
                retry=f"{retry_count + 1}/{MAX_RETRIES}",
                wait_sec=delay,
                result_ids=[r.get("ResultID") for r in records]
            )
            time.sleep(delay)
            return safe_process_batch(cursor, conn, records, retry_count + 1)

        # ❌ Other error or retries exhausted
        log("error", "❌ BATCH FAILED",
            size=len(records),
            error=error_str,
            is_deadlock=is_deadlock,
            retry=retry_count,
            result_ids=[r.get("ResultID") for r in records]
        )

        # Single bad record → log full record and skip
        if len(records) == 1:
            log("error", "🚨 BAD RECORD — DROPPED",
                result_id=records[0].get("ResultID"),
                location_id=records[0].get("LocationID"),
                db_status=records[0].get("DbStatus"),
                record=records[0]
            )
            return False, conn

        # Split batch into halves
        mid   = len(records) // 2
        left  = records[:mid]
        right = records[mid:]

        log("info", "🔀 Splitting batch",
            original_size=len(records),
            left=len(left),
            right=len(right)
        )

        _, conn = safe_process_batch(cursor, conn, left)
        _, conn = safe_process_batch(cursor, conn, right)
        return False, conn

# -----------------------------
# CONSUMER
# -----------------------------
def consume():
    conn   = get_connection_with_retry()
    cursor = conn.cursor()

    broker_list = [b.strip() for b in KAFKA_SERVERS.split(",") if b.strip()]
    log("info", "🔌 Connecting to Kafka", brokers=broker_list)

    consumer = KafkaConsumer(
        TABLE_NAME,
        bootstrap_servers=broker_list,
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        enable_auto_commit=False,
        group_id=GROUP_ID,

        # Performance
        max_poll_records=1000,
        fetch_max_bytes=52428800,
        fetch_max_wait_ms=500,

        # Session
        session_timeout_ms=30000,
        heartbeat_interval_ms=10000,
        request_timeout_ms=40000,

        auto_offset_reset="latest",

        # Reconnect
        reconnect_backoff_ms=1000,
        reconnect_backoff_max_ms=30000,
    )

    log("info", "🚀 TP Consumer started", group=GROUP_ID, brokers=broker_list)

    poll_cycle    = 0
    total_written = 0
    total_dropped = 0

    while running:
        batch = consumer.poll(timeout_ms=5000)

        if not batch:
            # Ping DB on every idle poll to detect outage early
            conn   = ensure_connection(conn)
            cursor = conn.cursor()
            continue

        poll_cycle  += 1
        all_records  = []
        poll_total   = sum(len(msgs) for msgs in batch.values())

        log("info", "📨 Poll received", cycle=poll_cycle, messages=poll_total)

        try:
            for _, messages in batch.items():
                for msg in messages:
                    all_records.append(msg.value)

                    if len(all_records) >= BATCH_SIZE:
                        success, conn = safe_process_batch(cursor, conn, all_records)
                        cursor = conn.cursor()
                        if success:
                            consumer.commit()
                            total_written += len(all_records)
                            log("info", "✅ Mid-batch committed",
                                size=len(all_records),
                                total_written=total_written)
                        else:
                            total_dropped += len(all_records)
                            log("warning", "⚠️ Mid-batch partially failed",
                                size=len(all_records),
                                total_dropped=total_dropped)
                        all_records.clear()

            # Leftover records
            if all_records:
                success, conn = safe_process_batch(cursor, conn, all_records)
                cursor = conn.cursor()
                if success:
                    consumer.commit()
                    total_written += len(all_records)
                    log("info", "✅ Final batch committed",
                        size=len(all_records),
                        total_written=total_written)
                else:
                    total_dropped += len(all_records)
                    log("warning", "⚠️ Final batch partially failed",
                        size=len(all_records),
                        total_dropped=total_dropped)
                all_records.clear()

        except Exception as e:
            log("error", "❌ POLL LOOP ERROR", error=str(e))
            try:
                conn.rollback()
            except:
                pass
            conn   = ensure_connection(conn)
            cursor = conn.cursor()

    consumer.close()
    conn.close()
    log("info", "🛑 Consumer shut down cleanly",
        total_written=total_written,
        total_dropped=total_dropped)

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
            log("error", "CONNECTION ERROR",
                error=str(e),
                retry=retry,
                next_retry_sec=delay)

        except KafkaError as e:
            delay = 15
            log("error", "KAFKA ERROR",
                error=str(e),
                next_retry_sec=delay)

        except Exception as e:
            delay = 30
            retry += 1
            log("error", "UNEXPECTED ERROR",
                error=str(e),
                retry=retry,
                next_retry_sec=delay)

        if running:
            time.sleep(delay)