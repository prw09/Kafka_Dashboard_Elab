import pyodbc
import logging
import time
import smtplib
import json
import signal
import os
from kafka import KafkaConsumer
from datetime import datetime
from email.mime.text import MIMEText
from kafka.errors import KafkaError, NoBrokersAvailable, CommitFailedError

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('consumer')

# Email configuration
EMAIL_SENDER = 'defaulterdyp@gmail.com'
EMAIL_PASSWORD = 'qaxh wfxe vxjr rwqg'
EMAIL_RECEIVER = 'shreyas.phansalkar2017@gmail.com'
SMTP_SERVER = 'smtp.gmail.com'
SMTP_PORT = 587

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
    "dbo.KafkaBrokerStatus": ['ID', 'LocationID', 'logType', 'issync', 'created_at', 'updated_at', 'IsRunning']
}


# Function to send an email alert or notification using SMTP from central server
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
    os._exit(0)  # Force exit to break retry loop


def handle_shutdown(signum, frame):
    logger.info("System is shutting down.")
    send_email("Consumer Shutdown", "The system is shutting down. Check consumer status.")
    os._exit(0)  # Force exit to break retry loop


# Process a single Kafka message
def process_message(cursor, message):
    record = message.value
    logger.info(f"Processing message from {message.topic}")

    table_name = message.topic
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
                if record.get('DbStatus', 0) <= current_db_status:
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


# Main consumer function
def consume_messages(consumer_group_id):
    consumer = None
    conn = None

    try:
        # Initialize database connection
        conn = pyodbc.connect(
            "DRIVER={ODBC Driver 18 for SQL Server};"
            "SERVER=GGNELAB;"
            "DATABASE=Central_Pathkind_BPHT;"
            "UID=sa;"
            "PWD=Labsoul@2024;"
            "Encrypt=yes;"
            "TrustServerCertificate=yes;"
        )
        cursor = conn.cursor()

        # Initialize Kafka consumer (excluding LogException)
        consumer = KafkaConsumer(
            *tables_columns.keys(),
            bootstrap_servers='172.16.1.30:9095',
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

        while True:
            batch = consumer.poll(timeout_ms=10000, max_records=100)
            if not batch:
                continue

            try:
                for topic_partition, messages in batch.items():
                    for message in messages:
                        success = process_message(cursor, message)
                        if success:
                            consumer.commit()
                        else:
                            logger.error(f"Failed to process message: {message.value}")

                conn.commit()
                print(message.value)
                logger.info(f"Processed {len(messages)} messages successfully")
            except (pyodbc.OperationalError, KafkaError) as e:
                logger.error(f"Critical error during processing: {str(e)}")
                conn.rollback()
                raise  # Trigger restart
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

    consumer_group_id = 'fourth_gp'
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