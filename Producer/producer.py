import json
import pyodbc
import logging
import os
from kafka import KafkaProducer
from kafka.errors import KafkaError
from datetime import datetime, timedelta
import time
import sys
import signal
import threading
from logging.handlers import TimedRotatingFileHandler

# -----------------------------
# GLOBAL FLAG
# -----------------------------
running = True

# -----------------------------
# LOGGING CONSTANTS
# -----------------------------
LOG_BACKUP_COUNT = 24 * 30    # 24 hourly files × 30 days retention
LOG_FLUSH_INTERVAL_SECONDS = 24 * 3600

# -----------------------------
# EXECUTION DIRECTORY
# -----------------------------
if getattr(sys, 'frozen', False):
    exe_dir = os.path.dirname(sys.executable)
else:
    exe_dir = os.path.dirname(os.path.abspath(__file__))

# -----------------------------
# LOGGING SETUP
# -----------------------------
logs_dir = os.path.join(exe_dir, 'logs')
os.makedirs(logs_dir, exist_ok=True)

current_day = datetime.now().strftime('%Y-%m-%d')
handler     = None


def _make_day_log_dir(day_str: str):
    day_dir = os.path.join(logs_dir, day_str)
    os.makedirs(day_dir, exist_ok=True)
    return day_dir


def _setup_log_handler_for_day(day_str: str):
    global handler
    day_dir      = _make_day_log_dir(day_str)
    log_file_path = os.path.join(day_dir, "producer.log")

    root_logger = logging.getLogger('source_db_logger')
    if handler is not None:
        try:
            root_logger.removeHandler(handler)
            handler.close()
        except Exception:
            pass

    new_handler = TimedRotatingFileHandler(
        log_file_path,
        when='H',
        interval=1,
        backupCount=LOG_BACKUP_COUNT,
        encoding='utf-8'
    )
    new_handler.suffix = "%Y-%m-%d_%H.log"
    new_handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s'))

    handler = new_handler
    root_logger.addHandler(handler)


source_logger = logging.getLogger('source_db_logger')
source_logger.setLevel(logging.DEBUG)
_setup_log_handler_for_day(current_day)


def _watch_day_rollover():
    """Background thread — moves log handler to a new day folder at midnight."""
    global current_day
    while running:
        now_day = datetime.now().strftime('%Y-%m-%d')
        if now_day != current_day:
            try:
                current_day = now_day
                _setup_log_handler_for_day(current_day)
                source_logger.info(f"Log handler moved to new day folder: {current_day}")
            except Exception as e:
                print(f"Failed to rotate log handler for new day: {e}")
        for _ in range(60):
            if not running:
                break
            time.sleep(1)


def _periodic_log_flush():
    """Background thread — flushes log handler to disk periodically."""
    while running:
        try:
            if handler is not None:
                handler.flush()
        except Exception:
            pass
        for _ in range(int(LOG_FLUSH_INTERVAL_SECONDS)):
            if not running:
                break
            time.sleep(1)

# -----------------------------
# LOAD CONFIG
# -----------------------------
config_file_path = os.path.join(exe_dir, 'config.json')
try:
    with open(config_file_path, 'r') as config_file:
        config = json.load(config_file)
except Exception as e:
    source_logger.error(f"Error loading configuration file: {e}")
    sys.exit(1)

try:
    source_conn_params = config["source_conn_params"]
    kafka_broker       = config["kafka_broker"]
    tables             = config["tables"]
    producer_id        = config.get("producer_id")
    producer_name      = config.get("producer_name")
    location_id        = config.get("location_id")
except KeyError as e:
    source_logger.error(f"Missing required configuration key: {e}")
    sys.exit(1)

# -----------------------------
# SIGNAL HANDLING
# -----------------------------
def handle_signal(sig, frame):
    global running
    source_logger.info("Received termination signal, shutting down gracefully...")
    running = False

signal.signal(signal.SIGINT, handle_signal)
signal.signal(signal.SIGTERM, handle_signal)

# -----------------------------
# KAFKA PRODUCER — created once, reused across all cycles
#
# FIX: previously a new KafkaProducer was created and destroyed inside
# fetch_and_send_data on every 5-second poll cycle, creating and tearing
# down a full broker TCP connection each time. This is wasteful and can
# cause delivery gaps during reconnect. The producer is now created once
# at startup and shared across all calls.
#
# acks="all"  — broker confirms all replicas received the message before
#               returning. Combined with future.get(timeout) this means
#               issync is only set to 1 after guaranteed delivery.
# retries=5   — automatic retry on transient broker errors.
# linger_ms=20 — small batching window; improves throughput with minimal
#                latency cost.
# -----------------------------
def _create_kafka_producer():
    return KafkaProducer(
        bootstrap_servers=kafka_broker,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else b"",
        acks="all",
        retries=5,
        linger_ms=20,
    )

kafka_producer = None   # initialised in main() after logging is ready

# -----------------------------
# HEARTBEAT THREAD
# -----------------------------
def send_heartbeat(producer_id, producer_name, location_id, kafka_broker,
                   interval_seconds=30):
    """
    Sends a lightweight heartbeat message every interval_seconds so the
    central monitoring service knows this producer is alive.
    Uses its own dedicated KafkaProducer so heartbeat delivery is never
    blocked by main-thread flushing.
    """
    heartbeat_topic    = 'producer_heartbeat'
    hb_kafka_producer  = None
    try:
        hb_kafka_producer = KafkaProducer(
            bootstrap_servers=kafka_broker,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

        while running:
            heartbeat_message = {
                'producer_id':   producer_id,
                'producer_name': producer_name,
                'location_id':   location_id,
                'timestamp':     datetime.now().isoformat()
            }
            try:
                hb_kafka_producer.send(heartbeat_topic, heartbeat_message)
                hb_kafka_producer.flush(timeout=10)
                source_logger.debug(f"Sent heartbeat: {heartbeat_message}")
            except Exception as e:
                source_logger.error(f"Failed to send heartbeat: {e}")

            for _ in range(int(interval_seconds)):
                if not running:
                    break
                time.sleep(1)

    except Exception as e:
        source_logger.error(f"Heartbeat thread failed to start: {e}")
    finally:
        if hb_kafka_producer:
            try:
                hb_kafka_producer.close()
            except Exception:
                pass

# -----------------------------
# FETCH AND SEND
#
# FIX 1 — LocationID key on every message:
#   kafka_producer.send(..., key=str(LocationID)) ensures Kafka routes all
#   messages for the same LocationID to the same partition. Combined with
#   Kafka's single-consumer-per-partition guarantee, this means no two
#   consumer processes ever write the same LocationID's rows concurrently,
#   eliminating cross-process row contention entirely.
#
# FIX 2 — issync only set AFTER confirmed Kafka delivery:
#   Previously issync was marked 1 immediately after send() even if the
#   message was never actually delivered (network blip, buffer full).
#   The record was then permanently lost — marked synced in the source DB
#   but never written to the central DB. Now future.get(timeout=10) blocks
#   until the broker confirms receipt. issync is only updated on success.
# -----------------------------
def fetch_and_send_data(table_name, check_dbstatus=False, exclude_columns=None):
    global kafka_producer

    if exclude_columns is None:
        exclude_columns = []

    source_logger.debug(f"Connecting to source database for table {table_name}")

    conn   = None
    cursor = None

    try:
        conn_str = (
            f"DRIVER={{ODBC Driver 13 for SQL Server}};"
            f"SERVER={source_conn_params['host']},{source_conn_params['port']};"
            f"DATABASE={source_conn_params['dbname']};"
            f"UID={source_conn_params['user']};"
            f"PWD={source_conn_params['password']}"
        )
        conn   = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        source_logger.info(f"Connected to source DB — fetching from {table_name}")

        # ------------------------------------------------------------------
        # Build query — only unsynchronised rows from the last 3 days
        # ------------------------------------------------------------------
        three_days_ago = (datetime.now() - timedelta(days=3)).strftime('%Y-%m-%d')

        if table_name == "dbo.Patient_Details":
            query = (
                f"SELECT * FROM {table_name} "
                f"WHERE issync = 0 AND CreateDate >= '{three_days_ago}'"
            )
        elif table_name in ("dbo.Orders", "dbo.Test_Parameters"):
            query = (
                f"SELECT * FROM {table_name} "
                f"WHERE issync = 0 AND CreatedDate >= '{three_days_ago}'"
            )
        elif table_name == "dbo.UtilityException":
            query = (
                f"SELECT * FROM {table_name} "
                f"WHERE issync = 0 AND Timestamp >= '{three_days_ago}'"
            )
        else:
            query = f"SELECT * FROM {table_name} WHERE issync = 0"

        if check_dbstatus:
            query += " AND DbStatus BETWEEN 1 AND 5"

        cursor.execute(query)
        rows    = cursor.fetchall()
        col_names = [desc[0] for desc in cursor.description]

        source_logger.info(f"Fetched {len(rows)} rows from {table_name}")

        # Identify primary key column
        primary_key_column = 'ResultID' if 'ResultID' in col_names else col_names[0]

        sent_count    = 0
        skipped_count = 0

        for row in rows:
            if not running:
                break

            # Build record dict, skipping excluded columns
            record = {}
            for col, val in zip(col_names, row):
                if col in exclude_columns:
                    continue
                record[col] = val.isoformat() if isinstance(val, datetime) else val

            pk_value = record.get(primary_key_column)
            if pk_value is None:
                source_logger.warning(
                    f"Primary key {primary_key_column} is NULL — skipping record"
                )
                skipped_count += 1
                continue

            # ----------------------------------------------------------
            # FIX 1: key every message by LocationID
            # All messages for the same LocationID go to the same Kafka
            # partition → owned by exactly one consumer process → no
            # cross-process row contention on the central DB.
            # ----------------------------------------------------------
            msg_key = str(record.get("LocationID", ""))

            # ----------------------------------------------------------
            # FIX 2: wait for broker acknowledgement before marking
            # issync=1. If delivery fails, the record stays issync=0 in
            # the source DB and will be retried on the next cycle.
            # ----------------------------------------------------------
            try:
                # Recreate producer if it was closed after a previous error
                if kafka_producer is None:
                    source_logger.warning("Kafka producer is None — recreating...")
                    kafka_producer = _create_kafka_producer()

                future = kafka_producer.send(
                    table_name,
                    key=msg_key,
                    value=record       # value_serializer handles encoding
                )
                future.get(timeout=10)   # blocks until broker confirms

                source_logger.debug(
                    f"Kafka delivery confirmed | table={table_name} "
                    f"| {primary_key_column}={pk_value} "
                    f"| LocationID={record.get('LocationID')}"
                )

            except KafkaError as e:
                source_logger.error(
                    f"Kafka delivery FAILED — issync NOT updated "
                    f"| {primary_key_column}={pk_value} | error={e}"
                )
                skipped_count += 1
                # Recreate the producer so the next record gets a fresh attempt
                try:
                    kafka_producer.close()
                except Exception:
                    pass
                kafka_producer = None
                continue   # do NOT mark issync=1

            except Exception as e:
                source_logger.error(
                    f"Unexpected send error — issync NOT updated "
                    f"| {primary_key_column}={pk_value} | error={e}"
                )
                skipped_count += 1
                continue   # do NOT mark issync=1

            # Only mark issync=1 after confirmed Kafka delivery
            try:
                update_query = (
                    f"UPDATE {table_name} SET issync = 1 "
                    f"WHERE {primary_key_column} = ?"
                )
                cursor.execute(update_query, (pk_value,))
                conn.commit()
                sent_count += 1
                source_logger.debug(
                    f"issync updated | {primary_key_column}={pk_value}"
                )
            except Exception as e:
                source_logger.error(
                    f"issync update failed | {primary_key_column}={pk_value} | error={e}"
                )

        source_logger.info(
            f"Cycle complete | table={table_name} "
            f"| sent={sent_count} | skipped={skipped_count}"
        )

    except Exception as e:
        source_logger.error(f"Error in fetch_and_send_data for {table_name}: {e}")

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


# -----------------------------
# MAIN
# -----------------------------
def main():
    global kafka_producer

    source_logger.info("Starting producer")

    # Create the shared Kafka producer once at startup
    try:
        kafka_producer = _create_kafka_producer()
        source_logger.info(f"Kafka producer connected | brokers={kafka_broker}")
    except Exception as e:
        source_logger.error(f"Failed to create Kafka producer: {e}")
        sys.exit(1)

    # Background: flush logs to disk periodically
    flush_thread = threading.Thread(target=_periodic_log_flush, daemon=True)
    flush_thread.start()

    # Background: roll log handler over to new day folder at midnight
    day_rollover_thread = threading.Thread(target=_watch_day_rollover, daemon=True)
    day_rollover_thread.start()

    # Background: send producer heartbeat every 30 seconds
    heartbeat_thread = threading.Thread(
        target=send_heartbeat,
        args=(producer_id, producer_name, location_id, kafka_broker),
        daemon=True
    )
    heartbeat_thread.start()

    try:
        while running:
            for table_name, table_config in tables.items():
                if not running:
                    break
                fetch_and_send_data(
                    table_name,
                    check_dbstatus=table_config.get("check_dbstatus", False),
                    exclude_columns=table_config.get("exclude_columns", [])
                )
            source_logger.debug(
                f"Cycle finished at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} "
                f"— waiting 5s"
            )
            time.sleep(5)

    except Exception as e:
        source_logger.error(f"Fatal error in main loop: {e}")

    finally:
        source_logger.info("Shutting down producer...")
        running_flag = False  # noqa — signal threads
        time.sleep(1)
        if kafka_producer:
            try:
                kafka_producer.flush(timeout=30)   # drain any buffered messages
                kafka_producer.close()
                source_logger.info("Kafka producer closed cleanly")
            except Exception as e:
                source_logger.error(f"Error closing Kafka producer: {e}")


if __name__ == "__main__":
    main()