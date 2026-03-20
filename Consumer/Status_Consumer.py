import json
import pyodbc
import logging
import time
import smtplib
import signal
import os
from kafka import KafkaConsumer
from Kafka import KafkaProducer
from datetime import datetime, timedelta
from email.mime.text import MIMEText
from kafka.errors import KafkaError, NoBrokersAvailable, CommitFailedError
from flask import Flask, jsonify
from flask_cors import CORS
import threading
from dotenv import load_dotenv
from packaging.tags import AppleVersion


# loading env variables
load_dotenv()

# global variables
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
PRODUCER_TIMEOUT_SECONDS=os.getenv("PRODUCER_TIMEOUT_SECONDS")


# Initialize logging
from logging.handlers import TimedRotatingFileHandler


# Global variables


LOG_DIR = os.path.join(os.path.dirname(__file__), "logs")
os.makedirs(LOG_DIR, exist_ok=True)

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

LOG_FORMAT = (
    "%(asctime)s | %(levelname)s | %(process)d | "
    "%(threadName)s | %(filename)s:%(lineno)d | %(message)s"
)

formatter = logging.Formatter(LOG_FORMAT)

file_handler = TimedRotatingFileHandler(
    filename=os.path.join(LOG_DIR, "consumer2.log"),
    when="H",
    interval=12,
    backupCount=24,
    encoding="utf-8"
)
file_handler.setFormatter(formatter)
file_handler.setLevel(LOG_LEVEL)

error_handler = TimedRotatingFileHandler(
    filename=os.path.join(LOG_DIR, "consumer2_error.log"),
    when="H",
    interval=12,
    backupCount=24,
    encoding="utf-8"
)
error_handler.setFormatter(formatter)
error_handler.setLevel(logging.ERROR)

console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
console_handler.setLevel(LOG_LEVEL)

logger = logging.getLogger("KafkaConsumer2Service")
logger.setLevel(LOG_LEVEL)
logger.handlers.clear()
logger.addHandler(file_handler)
logger.addHandler(error_handler)
logger.addHandler(console_handler)
logger.propagate = False


# Email configuration
EMAIL_SENDER = os.getenv("EMAIL_SENDER")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
EMAIL_RECEIVER = os.getenv("EMAIL_RECEIVER")
SMTP_SERVER = os.getenv("SMTP_SERVER")
SMTP_PORT = os.getenv("SMTP_PORT")

# Database connection parameters
DB_DRIVER13 = os.getenv("DB_DRIVER13")
DB_SERVER = os.getenv("DB_SERVER")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# Timeout for considering a producer dead (in seconds)
timeout = 60  # 1 minute timeout

# Initialize Flask app
app = Flask(__name__)
CORS(app)

tables_columns = {
    "dbo.LogException": ['ID', 'MachineFID', 'MessageString', 'Timestamp', 'LogType', 'ErrorCode', 'ModifiedDate',
                         'LocationID', 'IsSync'],
    "dbo.Machine": ['ID', 'MachineName', 'LocationID', 'CategoryID', 'ConnectionMode', 'QCStatus', 'CreateDate',
                    'ModifiedDate', 'IsSync', 'InstrumentId', 'IsDeleted'],
    "dbo.QCIntegrationTable": [
        'QCID', 'SenderName', 'DateTimeofResult', 'LotID', 'ControlLevel',
        'QCBottleNo', 'ActionCode', 'SampleType', 'Parameter', 'Dilution',
        'ResultValue', 'Unit', 'ReferenceRange', 'Flags', 'RerunFlags',
        'OperatorID', 'Comment1', 'Comment2', 'MachineFid', 'InstrumentID',
        'LocationId', 'Dbstatus', 'CreatedDate', 'Modifieddate', 'IsSync'
    ],
    "dbo.AppVersionLog": ['Id','InstallationVersionNumber','InstallationSystemName','UserName','InstrumentName',
                          'LocationName','CenterId','LogDate','BuildVersion','BuildDate','IsSync'],
    "dbo.MachineMapping": ['Id','MachineFId','ConfigMachineDataFID','ParameterFId','pFrom','pTo','TestParamFID','Expression','DecimalPlaces','CheckField',
                           'test','postfix','SampleType','IsDeleted','CreateDate','ModifiedDate','TestFID','LocationID','IsSync']
}


# Function to get database connection
def get_db_connection():

    conn = pyodbc.connect(
        f"DRIVER={DB_DRIVER13};"
        f"SERVER={DB_SERVER};"
        f"DATABASE={DB_NAME};"
        f"UID={DB_USER};"
        f"PWD={DB_PASSWORD}"
    )
    return conn


# Function to update producer status in the database
def update_producer_status(producer_id, producer_name, location_id, last_heartbeat, status):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            MERGE INTO ProducerStatus AS target
            USING (VALUES (?, ?, ?, ?, ?)) AS source (producer_id, producer_name, location_id, last_heartbeat, status)
            ON target.producer_id = source.producer_id
            WHEN MATCHED THEN
                UPDATE SET target.producer_name = source.producer_name,
                           target.location_id = source.location_id,
                           target.last_heartbeat = source.last_heartbeat,
                           target.status = source.status
            WHEN NOT MATCHED THEN
                INSERT (producer_id, producer_name, location_id, last_heartbeat, status)
                VALUES (source.producer_id, source.producer_name, source.location_id, source.last_heartbeat, source.status);
        """, producer_id, producer_name, location_id, last_heartbeat, status)
        conn.commit()
    except pyodbc.Error as e:
        logger.error(f"Database error: {str(e)}")
    finally:
        cursor.close()
        conn.close()


# Function to mark producers as dead if they haven't sent a heartbeat within the timeout period
def mark_producers_as_dead():
    while True:
        conn = get_db_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("""
                UPDATE ProducerStatus
                SET status = 0
                WHERE last_heartbeat < ?
            """, datetime.now() - timedelta(seconds=timeout))
            print()
            conn.commit()
        except pyodbc.Error as e:
            logger.error(f"Database error: {str(e)}")
        finally:
            cursor.close()
            conn.close()
        time.sleep(timeout)  # Run this cleanup every timeout seconds


# Function to monitor producer heartbeat
def monitor_producer_heartbeat():
    consumer = KafkaConsumer(
        'producer_heartbeat',
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        #api_version=(2, 7),
        enable_auto_commit=False,
        auto_offset_reset='latest',
        group_id='heartbeat_monitor',
        #max_poll_interval_ms=600000,
        session_timeout_ms=30000,
        heartbeat_interval_ms=10000
    )


    for message in consumer:
        heartbeat_message = message.value
        producer_id = heartbeat_message['producer_id']
        producer_name = heartbeat_message['producer_name']
        location_id = heartbeat_message['location_id']
        last_heartbeat = datetime.fromisoformat(heartbeat_message['timestamp'])
        timestamp_str = heartbeat_message['timestamp']

        status = 1  # Running

        print(f"Received heartbeat message: {heartbeat_message}")

        last_heartbeat = datetime.fromisoformat(timestamp_str)
        update_producer_status(producer_id, producer_name, location_id, last_heartbeat, status)

# Function to send email
def send_email(subject, body):
    msg = MIMEText(body)
    msg['Subject'] = subject
    msg['From'] = EMAIL_SENDER
    msg['To'] = EMAIL_RECEIVER
    try:
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(EMAIL_SENDER, EMAIL_PASSWORD)
            server.send_message(msg)
            logger.info(f"Email sent: {subject}")
    except Exception as e:
        logger.error(f"Failed to send email: {e}")


# Signal handling
def handle_interrupt(signum, frame):
    logger.info("Keyboard interrupt detected. Shutting down LogException/Machine consumer.")
    send_email("Consumer2 Interrupted Alert",
               "The consumer2.py process (LogException/Machine) has been interrupted. Please check the system status.")
    os._exit(0)


def handle_shutdown(signum, frame):
    logger.info("System is shutting down.")
    send_email("Consumer2 Shutdown", "The system is shutting down. Check LogException/Machine consumer status.")
    os._exit(0)


# Consume Kafka messages
def consume_messages():
    conn = pyodbc.connect(
        f"DRIVER={DB_DRIVER13};"
        f"SERVER={DB_SERVER};"
        f"DATABASE={DB_NAME};"
        f"UID={DB_USER};"
        f"PWD={DB_PASSWORD};"
    )

    cursor = conn.cursor()

    consumer = KafkaConsumer(
        "dbo.LogException", "dbo.Machine","dbo.AppVersionLog","dbo.MachineMapping",
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        #api_version=(2, 7),
        enable_auto_commit=False,
        auto_offset_reset='earliest',
        group_id='logexception_machine_sync',
        #max_poll_interval_ms=600000,
        session_timeout_ms=30000,
        heartbeat_interval_ms=10000
    )

    logger.info("LogException/Machine Consumer initialized. Starting message processing...")

    # Initialize Kafka producer for acks
    ack_producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    ACK_TOPIC = os.getenv("KAFKA_ACK_TOPIC", "ack-topic")

    while True:
        batch = consumer.poll(timeout_ms=5000, max_records=200)
        if not batch:
            continue

        messages = [msg for tp, msgs in batch.items() for msg in msgs]
        try:
            for message in messages:
                success = process_message(cursor, message)
                if not success:
                    raise Exception(f"Failed to process message: {message.value}")

            #FIRST commit DB
            conn.commit()

            #THEN commit Kafka offsets
            consumer.commit()

            logger.info(f"Processed {len(messages)} messages successfully")

        except Exception as e:
            logger.error(f"Error during processing: {str(e)}")
            conn.rollback()


# Process Kafka message
def process_message(cursor, message):

    """Processes an individual Kafka message for LogException, Machine, AppVersionLog, or MachineMapping."""
    record = message.value
    table_name = message.topic
    columns = tables_columns.get(table_name)
    if not columns:
        logger.error(f"Unknown table: {table_name}")
        return False

    # Convert date fields
    date_fields = ['ModifiedDate', 'Timestamp', 'CreateDate', 'LogDate']
    for key in date_fields:
        if key in record and isinstance(record[key], str):
            try:
                record[key] = datetime.fromisoformat(record[key])
            except (ValueError, TypeError):
                record[key] = None

    primary_key = columns[0]

    # Validate primary keys
    if table_name == "dbo.AppVersionLog":
        if record.get(primary_key) is None or record.get("CenterId") is None:
            logger.error("Missing Id or CenterId in dbo.AppVersionLog")
            return False
    else:
        if record.get(primary_key) is None or record.get("LocationID") is None:
            logger.error(f"Missing LocationID in {table_name}")
            return False

    try:
        # ============================
        # AppVersionLog Logic
        # ============================
        if table_name == "dbo.AppVersionLog":
            if record.get("IsSync") != 0:
                return True  # Already synced

            cursor.execute(
                "SELECT 1 FROM dbo.AppVersionLog WHERE Id = ? AND CenterId = ?",
                (record["Id"], record["CenterId"])
            )
            if cursor.fetchone():
                query = """
                    UPDATE dbo.AppVersionLog
                    SET InstallationVersionNumber = ?, InstallationSystemName = ?, UserName = ?,
                        InstrumentName = ?, LocationName = ?, LogDate = ?, BuildVersion = ?, BuildDate = ?, IsSync = 1
                    WHERE Id = ? AND CenterId = ?
                """
                params = (
                    record.get("InstallationVersionNumber"), record.get("InstallationSystemName"),
                    record.get("UserName"), record.get("InstrumentName"), record.get("LocationName"),
                    record.get("LogDate"), record.get("BuildVersion"), record.get("BuildDate"),
                    record["Id"], record["CenterId"]
                )
            else:
                query = """
                    INSERT INTO dbo.AppVersionLog (
                        Id, InstallationVersionNumber, InstallationSystemName, UserName,
                        InstrumentName, LocationName, CenterId, LogDate, BuildVersion, BuildDate, IsSync
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
                """
                params = (
                    record["Id"], record.get("InstallationVersionNumber"), record.get("InstallationSystemName"),
                    record.get("UserName"), record.get("InstrumentName"), record.get("LocationName"),
                    record["CenterId"], record.get("LogDate"), record.get("BuildVersion"), record.get("BuildDate")
                )
            cursor.execute(query, params)
            return True

        # ============================
        # MachineMapping Logic (FIXED)
        # ============================
        if table_name == "dbo.MachineMapping":

            # Skip already synced or deleted records
            if record.get("IsSync") != 0 or record.get("IsDeleted") == 1:
                logger.info(
                    f"Skipping MachineMapping record "
                    f"(MachineFId={record.get('MachineFId')}, "
                    f"TestParamFID={record.get('TestParamFID')}, "
                    f"LocationID={record.get('LocationID')})"
                )
                return True

            # Check existence using BUSINESS KEY (NOT Id)
            cursor.execute(
                """
                SELECT 1
                FROM dbo.MachineMapping
                WHERE MachineFId = ?
                  AND TestParamFID = ?
                  AND LocationID = ?
                """,
                (
                    record["MachineFId"],
                    record["TestParamFID"],
                    record["LocationID"]
                )
            )

            if cursor.fetchone():
                # UPDATE (do NOT overwrite CreateDate or Id)
                update_cols = [
                    f"{col} = ?"
                    for col in columns
                    if col not in ("Id", "CreateDate")
                ]

                query = f"""
                    UPDATE dbo.MachineMapping
                    SET {', '.join(update_cols)}
                    WHERE MachineFId = ?
                      AND TestParamFID = ?
                      AND LocationID = ?
                """

                params = (
                        [record.get(col, None) for col in columns if col not in ("Id", "CreateDate")]
                        + [
                            record["MachineFId"],
                            record["TestParamFID"],
                            record["LocationID"]
                        ]
                )
            else:
                # INSERT new mapping
                query = f"""
                    INSERT INTO dbo.MachineMapping
                    ({', '.join(columns)})
                    VALUES ({', '.join(['?'] * len(columns))})
                """
                params = [record.get(col, None) for col in columns]

            cursor.execute(query, params)
            return True

        # ============================
        # Generic tables (LogException, Machine, QCIntegrationTable, etc.)
        # ============================
        location_id = record["LocationID"]
        cursor.execute(f"SELECT 1 FROM {table_name} WHERE {primary_key} = ? AND LocationID = ?",
                       (record[primary_key], location_id))
        if cursor.fetchone():
            update_cols = [f"{col} = ?" for col in columns if col != primary_key]
            query = f"UPDATE {table_name} SET {', '.join(update_cols)} WHERE {primary_key} = ? AND LocationID = ?"
            params = [record.get(col, None) for col in columns if col != primary_key] + [record[primary_key], location_id]
        else:
            query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(['?'] * len(columns))})"
            params = [record.get(col, None) for col in columns]
        cursor.execute(query, params)
        return True

    except pyodbc.Error as e:
        logger.error(f"Database error in {table_name}: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Processing error in {table_name}: {str(e)}")
        return False

# Retries Ack from central consumer to producer
def send_ack_with_retry(ack_producer, topic, ack_message, max_retries=5):

    retry = 0
    while retry < max_retries:
        try:
            ack_producer.send(topic, ack_message)
            ack_producer.flush()
            logger.info(f"Ack sent for {ack_message['table']} ID {ack_message['record_id']}")
            return True

        except Exception as e:
            retry += 1
            delay = 2 ** retry  # exponential backoff
            logger.error(f"Failed to send ack (attempt {retry}/{max_retries}): {e}. Retrying in {delay}s...")
            time.sleep(delay)

    logger.error(f"Giving up sending ack after {max_retries} attempts for {ack_message['table']} ID {ack_message['record_id']}")
    # Optionally store ack_message in a persistent retry table for manual or background retries
    return False


if __name__ == "__main__":
    signal.signal(signal.SIGINT, handle_interrupt)
    signal.signal(signal.SIGTERM, handle_shutdown)

    # Start the heartbeat monitoring in a separate thread
    threading.Thread(target=monitor_producer_heartbeat, daemon=True).start()

    # Start the periodic cleanup of dead producers in a separate thread
    threading.Thread(target=mark_producers_as_dead, daemon=True).start()

    # Start the thread for Ack from central consumer to producer


    # Start the Kafka consumer
    consume_messages()
