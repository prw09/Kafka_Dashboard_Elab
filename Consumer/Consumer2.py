import json
import pyodbc
import logging
import time
import smtplib
import signal
import os
from kafka import KafkaConsumer
from datetime import datetime, timedelta
from email.mime.text import MIMEText
from kafka.errors import KafkaError, NoBrokersAvailable, CommitFailedError
from flask import Flask, jsonify
from flask_cors import CORS
import threading
import schedule
import shutil


# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('consumer_logexception')

# Email configuration
EMAIL_SENDER = 'defaulterdyp@gmail.com'
EMAIL_PASSWORD = 'qaxh wfxe vxjr rwqg'
EMAIL_RECEIVER = 'shreyas.phansalkar2017@gmail.com',
SMTP_SERVER = 'smtp.gmail.com'
SMTP_PORT = 587

# Database connection parameters
DB_DRIVER = '{ODBC Driver 13 for SQL Server}'
DB_SERVER = 'GGNELAB'
DB_NAME = 'Central_Pathkind_BPHT'
DB_USER = 'sa'
DB_PASSWORD = 'Labsoul@2024'

# Timeout for considering a producer dead (in seconds)
timeout = 60  # 1 minute timeout

# Initialize Flask app
app = Flask(__name__)
CORS(app)

# File Size Restriction Configuration
OUTPUT_DIR = "output_files"
MAX_FILE_SIZE_MB = 5
os.makedirs(OUTPUT_DIR, exist_ok=True)
file_index = 1
current_file = os.path.join(OUTPUT_DIR, f"kafka_data_part_{file_index}.json")


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
    ]
}


# Function to get database connection
def get_db_connection():
    conn = pyodbc.connect(
        f"DRIVER={DB_DRIVER};"
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
        bootstrap_servers='172.16.1.30:9095',
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


# Flask endpoint to get producer status
@app.route('/producer-status', methods=['GET'])
def get_producer_status():
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT producer_id, producer_name, location_id, status FROM ProducerStatus")
        rows = cursor.fetchall()
        status_response = {
            row.producer_id: {
                'producer_name': row.producer_name,
                'location_id': row.location_id,
                'status': row.status
            } for row in rows
        }
        return jsonify(status_response)
    except pyodbc.Error as e:
        logger.error(f"Database error: {str(e)}")
        return jsonify({}), 500
    finally:
        cursor.close()
        conn.close()


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
    '''conn = pyodbc.connect(
        "DRIVER={ODBC Driver 13 for SQL Server};"
        "SERVER=ELAB-SERVER\\SQLSERVER;"
        "DATABASE=Central_Pathkind_BPHT;"
        "UID=sa;"
        "PWD=P@t#k!n&#123$"
    )'''
    conn = pyodbc.connect(
        "DRIVER={ODBC Driver 13 for SQL Server};"
        "SERVER=GGNELAB;"
        "DATABASE=Central_Pathkind_BPHT;"
        "UID=sa;"
        "PWD=Labsoul@2024"
    )
    cursor = conn.cursor()

    consumer = KafkaConsumer(
        "dbo.LogException", "dbo.Machine",
        bootstrap_servers='172.16.1.30:9095',
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
    while True:
        batch = consumer.poll(timeout_ms=5000, max_records=200)
        if not batch:
            continue

        messages = [msg for tp, msgs in batch.items() for msg in msgs]
        try:
            for message in messages:
                success = process_message(cursor, message)
                if success:
                    consumer.commit()
                else:
                    logger.error(f"Failed to process message: {message.value}")
            conn.commit()
            logger.info(f"Processed {len(messages)} messages successfully")
        except Exception as e:
            logger.error(f"Error during processing: {str(e)}")
            conn.rollback()

    consumer.close()
    conn.close()


# Process Kafka message
def process_message(cursor, message):
    """ Processes an individual Kafka message for either LogException or Machine. """
    record = message.value
    table_name = message.topic
    columns = tables_columns.get(table_name)
    if not columns:
        logger.error(f"Unknown table: {table_name}")
        return False

    # Convert date fields
    date_fields = ['ModifiedDate', 'Timestamp', 'CreateDate']
    for key in date_fields:
        if key in record and isinstance(record[key], str):
            try:
                record[key] = datetime.fromisoformat(record[key])
            except (ValueError, TypeError):
                record[key] = None

    primary_key = columns[0]
    location_id = record.get('LocationID', None)
    if not location_id:
        logger.error(f"Missing LocationID in {table_name} record")
        return False

    try:
        cursor.execute(f"SELECT 1 FROM {table_name} WHERE {primary_key} = ? AND LocationID = ?",
                       (record[primary_key], location_id))
        if cursor.fetchone():
            update_cols = [f"{col} = ?" for col in columns if col != primary_key]
            query = f"UPDATE {table_name} SET {', '.join(update_cols)} WHERE {primary_key} = ? AND LocationID = ?"
            params = [record.get(col, None) for col in columns if col != primary_key] + [record[primary_key],
                                                                                         location_id]
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

# -------------------------------------------------------------------------
# LOG ROTATION MODULE (updated to 15 MB)
# -------------------------------------------------------------------------
LOG_DIR = r"E:\office\KafkaProject\Producer\logs"
BATKAFKA_DIR = r"E:\office\KafkaProject\Consumer\batkafka"

MAX_SIZE_BYTES = 15 * 1024 * 1024   # ✅ 15 MB
DELAY_MINUTES = 5  # rotate only if older than 5 minutes


def rotate_producer_log():
    """Rotate producer.log when >15MB and older than 5 minutes."""

    log_file = os.path.join(LOG_DIR, "producer.log")

    if not os.path.exists(log_file):
        print("⚠️ producer.log not found. Waiting...")
        return

    size = os.path.getsize(log_file)

    if size < MAX_SIZE_BYTES:
        return  # nothing to rotate

    modified_ts = os.path.getmtime(log_file)
    modified_time = datetime.fromtimestamp(modified_ts)
    age_minutes = (datetime.now() - modified_time).total_seconds() / 60

    if age_minutes < DELAY_MINUTES:
        return

    # Create date folder
    today_folder = datetime.now().strftime("%Y-%m-%d")
    today_path = os.path.join(BATKAFKA_DIR, today_folder)
    os.makedirs(today_path, exist_ok=True)

    # Create rotated file name
    timestamp = datetime.now().strftime("%H-%M-%S")
    rotated_name = f"producer_{timestamp}.log"
    dest_path = os.path.join(today_path, rotated_name)

    try:
        shutil.move(log_file, dest_path)
        print(f"📦 Rotated producer.log → {dest_path}")

        # Recreate empty producer.log
        open(log_file, 'w').close()
        print("🆕 New empty producer.log created")

    except Exception as e:
        print(f"❌ Log rotation error: {e}")


# -------------------------------------------------------------------------
# Runs rotation every 12 hours
# -------------------------------------------------------------------------
def log_rotation_scheduler_12hr():
    while True:
        print("⏳ Waiting for next 12-hour rotation window...")
        time.sleep(12 * 60 * 60)   # 12 hours
        rotate_producer_log()




if __name__ == "__main__":
    signal.signal(signal.SIGINT, handle_interrupt)
    signal.signal(signal.SIGTERM, handle_shutdown)

    # Start heartbeat monitor
    threading.Thread(target=monitor_producer_heartbeat, daemon=True).start()

    # Start dead-producer cleanup
    threading.Thread(target=mark_producers_as_dead, daemon=True).start()

    # Start log rotation every 12 hours
    threading.Thread(target=log_rotation_scheduler_12hr, daemon=True).start()

    # Start Flask API
    threading.Thread(target=lambda: app.run(host='0.0.0.0', port=5000, debug=False), daemon=True).start()

    # Start consumer (blocking)
    consume_messages()
