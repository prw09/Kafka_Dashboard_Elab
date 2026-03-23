import pyodbc
import logging
import time
import smtplib
import json
import signal
import os
from dotenv import load_dotenv
from kafka import KafkaConsumer
from kafka import KafkaProducer
from datetime import datetime
from email.mime.text import MIMEText
from kafka.structs import TopicPartition, OffsetAndMetadata
from kafka.errors import KafkaError, NoBrokersAvailable, CommitFailedError
from logging.handlers import TimedRotatingFileHandler


# Global Variables
running  = True

# loading env variables
load_dotenv()

# Configure logging
LOG_DIR = os.path.join(os.path.dirname(__file__), "logs")
os.makedirs(LOG_DIR, exist_ok=True)

LOG_FORMAT = (
    "%(asctime)s | %(levelname)s | %(process)d | "
    "%(filename)s:%(lineno)d | %(message)s"
)

formatter = logging.Formatter(LOG_FORMAT)

# Main log (rotates daily)
file_handler = TimedRotatingFileHandler(
    filename=os.path.join(LOG_DIR, "consumer.log"),
    when="H",
    interval=12,
    backupCount=24,
    encoding="utf-8"
)
file_handler.setFormatter(formatter)
file_handler.setLevel(logging.INFO)

# Error-only log
error_handler = TimedRotatingFileHandler(
    filename=os.path.join(LOG_DIR, "consumer_error.log"),
    when="H",
    interval=12,
    backupCount=24,
    encoding="utf-8"
)
error_handler.setFormatter(formatter)
error_handler.setLevel(logging.ERROR)

# Console handler (ONLY for manual/debug runs)
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
console_handler.setLevel(logging.INFO)

logger = logging.getLogger("KafkaConsumerService")
logger.setLevel(logging.INFO)

# Prevent duplicate logs
logger.handlers.clear()

logger.addHandler(file_handler)
logger.addHandler(error_handler)
logger.addHandler(console_handler)

logger.propagate = False



EMAIL_SENDER = os.getenv("EMAIL_SENDER")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
EMAIL_RECEIVER =os.getenv("EMAIL_RECEIVER")
SMTP_SERVER = os.getenv("SMTP_SERVER")
SMTP_PORT = os.getenv("SMTP_PORT")

# Mapping of tables to their columns (excluding LogException)
tables_columns = {
    "dbo.Patient_Details": ['PatientMasterID', 'PatientID', 'PatientName', 'DOB', 'Gender', 'PatLocationID',
                            'PatLocationName', 'CreateDate', 'ModifiedDate', 'LocationID', 'IsSync'],
    "dbo.Orders": ['OrderID', 'PatientMasterID', 'BarcodeNo', 'BarcodeNoID', 'TestCode', 'Sampletype', 'SampleTypeID',
                   'Samplecollectiontime', 'DbStatus', 'CreatedDate', 'ModifiedDate', 'LocationID', 'IsSync'],
    "dbo.Test_Parameters": ['ResultID', 'OrderID', 'PatientMasterID', 'MacDataGuid', 'ParameterCode', 'TestCode',
                            'Result', 'ResultType', 'DbStatus', 'CreatedDate', 'ResultReceivedDate', 'ResultUpdateDate',
                            'ModifiedDate', 'MachineFID', 'LocationID', 'IsSync', 'InstrumentId'],
    "dbo.ProductAuth_Table": ['AuthID', 'AuthKey', 'CreateDate', 'ModifiedDate', 'ExpTime', 'MacID', 'MachineName',
                              'LocationID', 'IsSync'],
    "dbo.Users": ['id', 'firstName', 'lastName', 'email', 'phoneNumber', 'location', 'role', 'userName',
                  'confirmUserName', 'password', 'confirmPassword', 'createdDate', 'modifiedDate', 'IsDeleted',
                  'IsActive', 'LocationID', 'IsSync'],
    "dbo.ExtTestCodeConfiguration": ['ID', 'LISParamId', 'LISParamName', 'IsDeleted', 'CreateDate', 'ModifiedDate',
                                     'LocationID', 'IsSync'],
    "dbo.UtilityException": ['ID', 'MessageString', 'ErrorCode', 'Timestamp', 'MachineFID', 'BarcodeNo', 'ModifiedDate',
                             'LocationID', 'IsSync'],
    "dbo.LocationMaster": ['LocationID', 'LocationName', 'CreateDate', 'ModifiedDate', 'CenterId', 'IsSync'],
    "dbo.KafkaBrokerStatus": ['ID', 'LocationID', 'logType', 'issync', 'created_at', 'updated_at', 'IsRunning'],
    "dbo.AppVersionLog": ['Id', 'InstallationVersionNumber', 'InstallationSystemName', 'UserName', 'InstrumentName',
                          'LocationName', 'CenterId', 'LogDate', 'BuildVersion', 'BuildDate', 'IsSync']
}


# Function to send emails
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


# Signal handlers for graceful shutdown
def handle_interrupt(signum, frame):
    logger.info("Keyboard interrupt detected. Shutting down.")
    send_email("Consumer Interrupted Alert",
               "The consumer process has been interrupted. Please check the system status.")
    # os._exit(0)  # Force exit to break retry loop


def handle_shutdown(signum, frame):
    logger.info("System is shutting down.")
    send_email("Consumer Shutdown", "The system is shutting down. Check consumer status.")
    #os._exit(0)  # Force exit to break retry loop

# json management
def log_json_issues(record, table_name):
    """
    Log important JSON fields and any missing critical fields.
    """
    # Log DbStatus and Result
    db_status = record.get('DbStatus')
    result = record.get('Result')
    result_id = record.get('ResultID')
    logger.info(f"JSON Fields - DbStatus: {db_status},ResultID: {result_id}, Result: {result}, Table: {table_name}")

    # Check for missing critical fields
    missing_fields = []
    for field in ['Result', 'ResultReceivedDate', 'CreatedDate']:
        if field not in record or record.get(field) in [None, '', 'null']:
            missing_fields.append(field)

    if missing_fields:
        logger.warning(f"Missing fields in record {record.get('ResultID', record.get('OrderID', 'N/A'))}: {missing_fields}")


# Process a single Kafka message
def process_message(cursor, message):
    record = message.value
    logger.info(f"Processing message from {message.topic}")
    table_name = message.topic

    # --- NEW: log JSON fields and missing critical fields ---
    log_json_issues(record, table_name)


    columns = tables_columns.get(table_name)
    if not columns:
        logger.error(f"Unknown table: {table_name}")
        return False

    # Date parsing logic
    date_fields = ['CreatedDate', 'Createdate', 'createdDate', 'modifiedDate',
                   'ModifiedDate', 'CreateDate', 'ResultReceivedDate',
                   'ResultUpdateDate', 'Timestamp', 'Samplecollectiontime',
                   'created_at', 'updated_at']
    for key in date_fields:
        if key in record and isinstance(record[key], str):
            try:
                record[key] = datetime.fromisoformat(record[key])
            except (ValueError, TypeError):
                record[key] = None

    # Prepare database operation
    primary_key = columns[0]
    location_id = record.get('LocationID')
    if not location_id:
        logger.error(f"Missing LocationID in {table_name} record")
        return False

    try:
        if 'DbStatus' in columns:
            cursor.execute(f"SELECT DbStatus FROM {table_name} WHERE {primary_key} = ? AND LocationID = ?",
                           (record[primary_key], location_id))
            result = cursor.fetchone()
            if result:
                current_db_status = result[0]
                if record.get('DbStatus', 0) < current_db_status:
                    logger.info(f"Skipping outdated DbStatus update for {primary_key} {record[primary_key]}")
                    return True

        # Build update/insert query
        placeholders = [record.get(col, None) for col in columns]
        if cursor.execute(f"SELECT 1 FROM {table_name} WHERE {primary_key} = ? AND LocationID = ?",
                          (record[primary_key], location_id)).fetchone():
            update_cols = [f"{col} = ?" for col in columns if col != primary_key]
            query = f"UPDATE {table_name} SET {', '.join(update_cols)} WHERE {primary_key} = ? AND LocationID = ?"
            params = placeholders[1:] + [placeholders[0], location_id]
        else:
            query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(['?'] * len(columns))})"
            params = placeholders

        cursor.execute(query, params)
        return True
    except pyodbc.Error as e:
        logger.error(f"Database error: {str(e)}")
        raise  # Re-raise to trigger transaction rollback
    except Exception as e:
        logger.error(f"Processing error: {str(e)}")
        return False


def consume_messages(consumer_group_id):
    consumer = None
    conn = None

    try:
        # Initialize database connection
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

        KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")

        ack_producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

        # Initialize Kafka consumer (excluding LogException)
        consumer = KafkaConsumer(
            *tables_columns.keys(),
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            # api_version=(2,7),
            enable_auto_commit=False,
            auto_offset_reset='earliest',
            group_id=consumer_group_id,
            # max_poll_interval_ms=600000,
            session_timeout_ms=30000,
            heartbeat_interval_ms=10000
        )
        logger.info("Consumer initialized. Starting message processing...")

        while running:
            batch = consumer.poll(timeout_ms=10000, max_records=100)
            if not batch:
                continue

            try:
                for topic_partition, messages in batch.items():
                    for message in messages:

                        success = process_message(cursor, message)

                        if success:
                            try:
                                record = message.value
                                primary_key = tables_columns[message.topic][0]

                                ack_message = {
                                    "table_name": message.topic,
                                    "primary_key": primary_key,
                                    "ResultID": record.get(primary_key),
                                    "location_id": record.get("LocationID"),
                                    "status": "SUCCESS",
                                    "timestamp": datetime.now().isoformat()
                                }

                                future = ack_producer.send("ack_topic", ack_message)
                                future.get(timeout=10)

                                # commit DB first
                                conn.commit()

                                # commit Kafka offset
                                tp = TopicPartition(message.topic, message.partition)
                                consumer.commit({tp: OffsetAndMetadata(message.offset + 1, None)})

                                logger.info(f"ACK SENT & COMMITTED: {record.get(primary_key)}")

                            except Exception as e:
                                conn.rollback()
                                logger.error(f"Failure: {e}")

                        else:
                            logger.error(f"Failed to process message: {message.value}")

            except (pyodbc.OperationalError, KafkaError) as e:
                logger.error(f"Critical error: {str(e)}")
                conn.rollback()
                raise

            except Exception as e:
                logger.error(f"Unexpected error: {str(e)}")
                conn.rollback()
                send_email("Consumer Processing Error", f"Error occurred: {str(e)}")
                continue

    finally:
        logger.info("Cleaning up resources...")
        if consumer:
            consumer.close()
        if conn:
            conn.close()

if __name__ == "__main__":

    signal.signal(signal.SIGINT, handle_interrupt)
    signal.signal(signal.SIGTERM, handle_shutdown)

    consumer_group_id = os.getenv("KAFKA_CONSUMER_GROUP")
    retry_count = 0
    max_retry_delay = 300  # 5 minutes

    while True:
        try:
            logger.info(f"Starting consumer (attempt {retry_count + 1})...")
            consume_messages(consumer_group_id)
        except (NoBrokersAvailable, CommitFailedError, pyodbc.OperationalError) as e:
            logger.error(f"Connection error: {str(e)}")
            delay = min(10 * (2 ** retry_count), max_retry_delay)
            retry_count += 1
        except KafkaError as e:
            logger.error(f"Kafka error: {str(e)}")
            delay = 15
            retry_count = 0
        except Exception as e:
            logger.error(f"Unexpected error: {str(e)}")
            delay = 30
            retry_count += 1

        logger.info(f"Restarting in {delay} seconds...")
        time.sleep(delay)