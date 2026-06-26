import os
import json
import time
import signal
import logging
import random
import threading
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

DEADLOCK_RETRIES        = 3
DLQ_RETRY_SLEEP_BASE_MS = 2000

DLQ_TABLE        = "dbo.Test_Parameters_DLQ"
DLQ_RETRY_AFTER  = int(os.getenv("DLQ_RETRY_AFTER_SECS", "600"))
DLQ_MAX_ATTEMPTS = int(os.getenv("DLQ_MAX_ATTEMPTS", "5"))

columns = [
    'ResultID', 'OrderID', 'PatientMasterID', 'MacDataGuid', 'ParameterCode', 'TestCode',
    'Result', 'ResultType', 'DbStatus', 'CreatedDate', 'ResultReceivedDate', 'ResultUpdateDate',
    'ModifiedDate', 'MachineFID', 'LocationID', 'IsSync', 'InstrumentId'
]

# -----------------------------
# LOGGING
# -----------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_DIR  = os.path.join(BASE_DIR, "logs_tp")
os.makedirs(LOG_DIR, exist_ok=True)

LOG_FORMAT         = "%(asctime)s | %(levelname)s | %(process)d | %(message)s"
INSTANCE_NAME      = os.getenv("TP_INSTANCE_NAME", f"tp_consumer_{os.getpid()}")
LOG_RETENTION_DAYS = int(os.getenv("LOG_RETENTION_DAYS", "3"))

logger = logging.getLogger(INSTANCE_NAME)
logger.setLevel(logging.INFO)
logger.handlers.clear()

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
            except Exception:
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

def is_deadlock(exc):
    return "40001" in str(exc)

def is_doomed(exc):
    return "3930" in str(exc)

def safe_rollback(conn):
    try:
        conn.rollback()
        return True
    except Exception as rb_exc:
        if is_doomed(rb_exc):
            logger.error("☠️  Transaction doomed (3930) — connection must be replaced")
            return False
        logger.error(f"Rollback failed unexpectedly: {rb_exc}")
        return False

def replace_connection(conn, cursor):
    try:
        cursor.close()
    except Exception:
        pass
    try:
        conn.close()
    except Exception:
        pass
    logger.info("🔄 Replacing DB connection...")
    new_conn   = get_connection()
    new_cursor = new_conn.cursor()
    logger.info("✅ DB connection replaced")
    return new_conn, new_cursor


# -----------------------------
# DEAD LETTER QUEUE
# -----------------------------

def ensure_dlq_table(cursor, conn):
    cursor.execute(f"""
    IF NOT EXISTS (
        SELECT 1 FROM sys.objects
        WHERE object_id = OBJECT_ID(N'{DLQ_TABLE}') AND type = 'U'
    )
    CREATE TABLE {DLQ_TABLE} (
        DLQId           BIGINT IDENTITY(1,1) PRIMARY KEY,
        ResultID        BIGINT,
        LocationID      INT,
        Payload         NVARCHAR(MAX),
        ErrorReason     NVARCHAR(500),
        Attempts        INT       DEFAULT 1,
        Failed          BIT       DEFAULT 0,
        CreatedAt       DATETIME2 DEFAULT GETDATE(),
        LastAttemptAt   DATETIME2 DEFAULT GETDATE(),
        ResolvedAt      DATETIME2 NULL
    )
    """)
    conn.commit()


def write_to_dead_letter(cursor, conn, record, error):
    try:
        payload   = json.dumps(record, default=str)
        error_str = str(error)[:500]

        cursor.execute(f"""
            IF EXISTS (
                SELECT 1 FROM {DLQ_TABLE}
                WHERE ResultID = ? AND LocationID = ? AND Failed = 0
            )
                UPDATE {DLQ_TABLE}
                SET    Attempts      = Attempts + 1,
                       ErrorReason   = ?,
                       LastAttemptAt = GETDATE()
                WHERE  ResultID = ? AND LocationID = ? AND Failed = 0
            ELSE
                INSERT INTO {DLQ_TABLE} (ResultID, LocationID, Payload, ErrorReason)
                VALUES (?, ?, ?, ?)
        """,
            record.get("ResultID"), record.get("LocationID"),
            error_str,
            record.get("ResultID"), record.get("LocationID"),
            record.get("ResultID"), record.get("LocationID"),
            payload, error_str
        )
        conn.commit()
        logger.warning(
            f"📥 DLQ | ResultID={record.get('ResultID')} "
            f"| LocationID={record.get('LocationID')} "
            f"| reason={error_str[:120]}"
        )
    except Exception as dlq_exc:
        logger.error(f"❌ DLQ write failed: {dlq_exc} — falling back to local file")
        _write_dlq_to_file(record, error)


def _write_dlq_to_file(record, error):
    try:
        dlq_file = os.path.join(LOG_DIR, f"{INSTANCE_NAME}_dlq.jsonl")
        entry = {
            "ts":     datetime.utcnow().isoformat(),
            "error":  str(error)[:500],
            "record": {k: str(v) for k, v in record.items()}
        }
        with open(dlq_file, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry) + "\n")
        logger.warning(f"📄 DLQ fallback file: {dlq_file}")
    except Exception as fe:
        logger.critical(f"💀 TOTAL DLQ FAILURE — record lost: {record} | {fe}")


# -----------------------------
# DLQ REPROCESS WORKER
# Runs in its own background thread with its own dedicated DB connection.
# Completely isolated from the main poll loop — no lock contention.
# -----------------------------

def _dlq_reprocess_worker():
    logger.info("🔁 DLQ reprocess thread started")

    while running:
        for _ in range(DLQ_RETRY_AFTER):
            if not running:
                break
            time.sleep(1)

        if not running:
            break

        try:
            dlq_conn   = get_connection()
            dlq_cursor = dlq_conn.cursor()
        except Exception as e:
            logger.error(f"DLQ thread: failed to open DB connection: {e}")
            continue

        try:
            _run_dlq_pass(dlq_conn, dlq_cursor)
        except Exception as e:
            logger.error(f"DLQ thread: unhandled error in reprocess pass: {e}")
        finally:
            try:
                dlq_cursor.close()
            except Exception:
                pass
            try:
                dlq_conn.close()
            except Exception:
                pass

    logger.info("🔁 DLQ reprocess thread stopped")


def _run_dlq_pass(dlq_conn, dlq_cursor):
    try:
        dlq_cursor.execute(f"""
            SELECT DLQId, Payload, Attempts
            FROM   {DLQ_TABLE}
            WHERE  Failed        = 0
            AND    Attempts      < ?
            AND    LastAttemptAt < DATEADD(SECOND, -{DLQ_RETRY_AFTER}, GETDATE())
        """, DLQ_MAX_ATTEMPTS)
        rows = dlq_cursor.fetchall()
    except Exception as e:
        logger.error(f"DLQ fetch failed: {e}")
        return

    if not rows:
        return

    logger.info(f"🔁 DLQ reprocess pass | {len(rows)} record(s) pending")

    for dlq_id, payload_json, attempts in rows:
        if not running:
            break

        try:
            record = json.loads(payload_json)
        except Exception:
            logger.error(f"DLQ row {dlq_id} unparseable — marking failed")
            _mark_dlq_failed(dlq_cursor, dlq_conn, dlq_id, "Unparseable payload")
            continue

        ok = _dlq_safe_process_one(dlq_conn, dlq_cursor, record)

        if ok:
            try:
                dlq_cursor.execute(f"""
                    UPDATE {DLQ_TABLE}
                    SET Failed=0, ResolvedAt=GETDATE()
                    WHERE DLQId=?
                """, dlq_id)
                dlq_conn.commit()
                logger.info(
                    f"✅ DLQ resolved | ResultID={record.get('ResultID')} "
                    f"| LocationID={record.get('LocationID')} "
                    f"| after {attempts + 1} total attempt(s)"
                )
            except Exception as e:
                logger.error(f"DLQ resolve update failed for DLQId={dlq_id}: {e}")
        else:
            new_attempts = attempts + 1
            if new_attempts >= DLQ_MAX_ATTEMPTS:
                _mark_dlq_failed(
                    dlq_cursor, dlq_conn, dlq_id,
                    f"Exhausted {DLQ_MAX_ATTEMPTS} attempts"
                )


def _dlq_safe_process_one(conn, cursor, record):
    last_exc = None

    for attempt in range(1, DEADLOCK_RETRIES + 1):
        try:
            process_batch(cursor, [record])
            conn.commit()
            logger.info(
                f"✅ DLQ batch committed | "
                f"ResultID={record.get('ResultID')} | attempt={attempt}"
            )
            return True

        except Exception as e:
            last_exc = e

            if is_doomed(e):
                logger.error(
                    f"☠️  DLQ doomed transaction (3930) | "
                    f"ResultID={record.get('ResultID')} | attempt={attempt} "
                    f"— replacing connection"
                )
                safe_rollback(conn)
                conn, cursor = replace_connection(conn, cursor)
                if attempt < DEADLOCK_RETRIES:
                    continue
                break

            if is_deadlock(e):
                safe_rollback(conn)
                if attempt < DEADLOCK_RETRIES:
                    sleep_ms = (DLQ_RETRY_SLEEP_BASE_MS * attempt) + random.randint(0, 500)
                    logger.warning(
                        f"⚠️ DLQ deadlock attempt {attempt}/{DEADLOCK_RETRIES} "
                        f"| ResultID={record.get('ResultID')} "
                        f"| retrying in {sleep_ms}ms"
                    )
                    time.sleep(sleep_ms / 1000.0)
                    continue
                break

            safe_rollback(conn)
            break

    logger.error(
        f"❌ DLQ record still failing after {DEADLOCK_RETRIES} attempts "
        f"| ResultID={record.get('ResultID')} | error={last_exc}"
    )
    return False


def _mark_dlq_failed(cursor, conn, dlq_id, reason):
    try:
        cursor.execute(f"""
            UPDATE {DLQ_TABLE}
            SET Failed=1, ErrorReason=?, LastAttemptAt=GETDATE()
            WHERE DLQId=?
        """, reason[:500], dlq_id)
        conn.commit()
        logger.error(
            f"🚨 DLQ PERMANENTLY FAILED | DLQId={dlq_id} | reason={reason} "
            f"— manual intervention required"
        )
    except Exception as e:
        logger.error(f"Could not mark DLQ row {dlq_id} as failed: {e}")


# -----------------------------
# safe_process_batch
#
# Three-layer defence against deadlocks:
#   1. Deduplicate — per (ResultID, LocationID) keep latest ModifiedDate.
#   2. Sort by (LocationID, ResultID) — deterministic lock order.
#   3. Retry up to DEADLOCK_RETRIES times with jitter on deadlock (40001).
#   4. Binary split fallback — size=1 failures go to DLQ.
# -----------------------------
def safe_process_batch(conn, cursor, records):

    # Deduplicate within batch — per (ResultID, LocationID)
    # keep the record with the latest ModifiedDate
    deduped = {}
    for r in records:
        key = (r.get("ResultID"), r.get("LocationID"))
        if key not in deduped:
            deduped[key] = r
        else:
            r_mod  = r.get("ModifiedDate") or ""
            ex_mod = deduped[key].get("ModifiedDate") or ""
            if r_mod > ex_mod:
                deduped[key] = r

    dupes = len(records) - len(deduped)
    if dupes > 0:
        logger.warning(
            f"⚠️ Deduplicated {dupes} duplicate(s) "
            f"| original={len(records)} | unique={len(deduped)}"
        )

    # Sort for deterministic lock ordering — reduces deadlocks
    sorted_records = sorted(
        deduped.values(),
        key=lambda r: (r.get("LocationID") or 0, r.get("ResultID") or 0)
    )

    pending = [sorted_records]
    all_ok  = True

    while pending:
        batch    = pending.pop()
        last_exc = None

        for attempt in range(1, DEADLOCK_RETRIES + 1):
            try:
                process_batch(cursor, batch)
                conn.commit()
                logger.info(f"✅ Batch committed | size={len(batch)} | attempt={attempt}")
                last_exc = None
                break

            except Exception as e:
                last_exc = e

                if is_doomed(e):
                    logger.error(
                        f"☠️  Doomed transaction (3930) | size={len(batch)} "
                        f"| attempt={attempt} — replacing connection"
                    )
                    safe_rollback(conn)
                    conn, cursor = replace_connection(conn, cursor)
                    if attempt < DEADLOCK_RETRIES:
                        continue
                    break

                if is_deadlock(e):
                    safe_rollback(conn)
                    if attempt < DEADLOCK_RETRIES:
                        sleep_ms = (50 * attempt) + random.randint(0, 50)
                        logger.warning(
                            f"⚠️ Deadlock attempt {attempt}/{DEADLOCK_RETRIES} "
                            f"| size={len(batch)} | retrying in {sleep_ms}ms"
                        )
                        time.sleep(sleep_ms / 1000.0)
                        continue
                    break

                safe_rollback(conn)
                break

        if last_exc is None:
            continue

        logger.error(
            f"❌ BATCH FAILED after {DEADLOCK_RETRIES} attempts "
            f"| size={len(batch)} | error={last_exc}"
        )

        if len(batch) == 1:
            write_to_dead_letter(cursor, conn, batch[0], last_exc)
            all_ok = False
            continue

        mid = len(batch) // 2
        pending.append(batch[mid:])
        pending.append(batch[:mid])

    return conn, cursor, all_ok


# -----------------------------
# process_batch
#
# MERGE RULES:
#   WHEN MATCHED AND source.DbStatus = 5
#       → UPDATE all columns — final state always wins
#   WHEN NOT MATCHED
#       → INSERT always regardless of DbStatus
#
# HOLDLOCK on MERGE — prevents phantom row deadlocks
# Same as SERIALIZABLE — safe for concurrent writers
# -----------------------------
def process_batch(cursor, records):

    merge_query = f"""
    MERGE {TABLE_NAME} WITH (HOLDLOCK) AS target
    USING (VALUES ({",".join(["?"] * len(columns))})) AS source ({",".join(columns)})
    ON target.ResultID    = source.ResultID
    AND target.LocationID = source.LocationID

    WHEN MATCHED AND source.DbStatus = 5
    THEN UPDATE SET
        {",".join([f"{col}=source.{col}" for col in columns if col not in ("ResultID","LocationID")])}

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

        if db_status is None:
            logger.error(
                f"⚠️ DbStatus is None | ResultID={record.get('ResultID')} "
                f"| LocationID={record.get('LocationID')} "
                f"| raw={raw_status!r} — skipping record"
            )
            continue

        logger.info(
            f"ResultID={record.get('ResultID')} | "
            f"LocationID={record.get('LocationID')} | "
            f"DbStatus={db_status}"
        )

        values.append([record.get(col) for col in columns])

    if not values:
        logger.info("No valid records to process in this batch")
        return

    cursor.fast_executemany = True
    cursor.executemany(merge_query, values)
    logger.info(f"Batch sent to DB | valid_records={len(values)}")


# -----------------------------
# CONSUMER
# -----------------------------
def consume():
    conn   = get_connection()
    cursor = conn.cursor()

    ensure_dlq_table(cursor, conn)

    # DLQ reprocess runs in its own background thread with its own
    # dedicated DB connection — isolated from main poll loop entirely
    dlq_thread = threading.Thread(
        target=_dlq_reprocess_worker,
        name="dlq-reprocess",
        daemon=True
    )
    dlq_thread.start()
    logger.info("🔁 DLQ reprocess thread launched")

    broker_list = [b.strip() for b in KAFKA_SERVERS.split(",") if b.strip()]
    logger.info(f"🔌 Connecting to Kafka brokers: {broker_list}")

    consumer = KafkaConsumer(
        TABLE_NAME,
        bootstrap_servers=broker_list,
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        enable_auto_commit=False,
        group_id=GROUP_ID,

        max_poll_records=1000,
        fetch_max_bytes=52428800,
        fetch_max_wait_ms=500,

        session_timeout_ms=30000,
        heartbeat_interval_ms=10000,
        request_timeout_ms=40000,

        auto_offset_reset="earliest",

        reconnect_backoff_ms=1000,
        reconnect_backoff_max_ms=30000,
    )

    logger.info(f"🚀 TP Consumer started | group={GROUP_ID} | brokers={broker_list}")

    DB_IDLE_CHECK_SECS = 300
    last_write_time    = time.monotonic()

    while running:
        batch = consumer.poll(timeout_ms=5000)

        if not batch:
            continue

        # DB idle heartbeat — reconnect if connection dropped
        idle_secs = time.monotonic() - last_write_time
        if idle_secs > DB_IDLE_CHECK_SECS:
            try:
                cursor.execute("SELECT 1")
                logger.debug(f"DB heartbeat OK after {idle_secs:.0f}s idle")
            except Exception:
                logger.warning(
                    f"DB connection lost after {idle_secs:.0f}s idle — reconnecting..."
                )
                conn, cursor = replace_connection(conn, cursor)

        all_records = []

        try:
            for _, messages in batch.items():
                for msg in messages:
                    all_records.append(msg.value)

                    if len(all_records) >= BATCH_SIZE:
                        conn, cursor, _ = safe_process_batch(conn, cursor, all_records)
                        consumer.commit()
                        last_write_time = time.monotonic()
                        logger.info(f"✅ Mid-batch committed: {len(all_records)}")
                        all_records.clear()

            if all_records:
                conn, cursor, _ = safe_process_batch(conn, cursor, all_records)
                consumer.commit()
                last_write_time = time.monotonic()
                logger.info(f"✅ Final batch committed: {len(all_records)}")
                all_records.clear()

        except Exception as e:
            logger.error(f"❌ POLL LOOP ERROR | {e}")
            rollback_ok = safe_rollback(conn)
            if not rollback_ok or is_doomed(e):
                conn, cursor = replace_connection(conn, cursor)

    consumer.close()
    conn.close()
    logger.info("🛑 Consumer shut down cleanly")


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