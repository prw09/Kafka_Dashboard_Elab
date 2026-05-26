import json
import pyodbc
import logging
import time
import smtplib
import signal
import os
from kafka import KafkaConsumer, KafkaProducer
from datetime import datetime, timedelta
from email.mime.text import MIMEText
from kafka.errors import KafkaError, NoBrokersAvailable, CommitFailedError
from flask import Flask
from flask_cors import CORS
import threading
from dotenv import load_dotenv


# loading env variables
load_dotenv()


# global variables
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
PRODUCER_TIMEOUT_SECONDS = os.getenv("PRODUCER_TIMEOUT_SECONDS")

# Initialize logging
from logging.handlers import TimedRotatingFileHandler

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

timeout = 60

# Initialize Flask app
app = Flask(__name__)
CORS(app)


tables_columns = {
    "dbo.LogException": [
        'ID', 'MachineFID',
        'MessageString', 'Timestamp',
        'LogType', 'ErrorCode', 'ModifiedDate',
        'LocationID', 'IsSync'
    ],

    "dbo.Machine": [
        'ID', 'MachineName', 'LocationID',
        'CategoryID', 'ConnectionMode',
        'QCStatus', 'CreateDate',
        'ModifiedDate', 'IsSync',
        'InstrumentId', 'IsDeleted'
    ],

    "dbo.QCIntegrationTable": [
        'QCID', 'SenderName', 'DateTimeofResult',
        'LotID', 'ControlLevel',
        'QCBottleNo', 'ActionCode', 'SampleType',
        'Parameter', 'Dilution',
        'ResultValue', 'Unit', 'ReferenceRange',
        'Flags', 'RerunFlags',
        'OperatorID', 'Comment1', 'Comment2',
        'MachineFid', 'InstrumentID',
        'LocationId', 'Dbstatus', 'CreatedDate',
        'Modifieddate', 'IsSync'
    ],

    "dbo.AppVersionLog": [
        'Id', 'InstallationVersionNumber',
        'InstallationSystemName',
        'UserName', 'InstrumentName',
        'LocationName', 'CenterId',
        'LogDate', 'BuildVersion',
        'BuildDate', 'IsSync'
    ],

    "dbo.MachineMapping": [
        'Id', 'MachineFId', 'ConfigMachineDataFID',
        'ParameterFId',
        'pFrom', 'pTo', 'TestParamFID',
        'Expression', 'DecimalPlaces',
        'CheckField', 'test', 'postfix',
        'SampleType', 'IsDeleted',
        'CreateDate', 'ModifiedDate', 'TestFID',
        'LocationID', 'IsSync'
    ],

    "dbo.MachineParameters": [
        'Id',
        'Parameter',
        'isDeleted',
        'ParmeterHeader',
        'MachineFID',
        'ConfigMachineDataFID',
        'ValueField',
        'TestField',
        'LISParamName',
        'CreateDate',
        'ModifiedDate',
        'LocationID'
    ],

    "ConfigMachineData": [
        'ConfigMachineDataId',
        'FormatDetails',
        'MachinFID',
        'TestName',
        'TestFID',
        'ResultMatchField',
        'MachineResultParam',
        'MachineResultSegment',
        'pFrom',
        'pTo',
        'OrderMatchField',
        'MachineOrdertParam',
        'MachineOrderSegment',
        'OrderLevel',
        'OrderFieldName',
        'CategoryFID',
        'TimeInterval',
        'Querymode',
        'SubResultSegment',
        'CreateDate',
        'ModifiedDate',
        'LocationID',
        'IsSync'
    ],

    "dbo.CategoryMaster": [
        'CategoryID',
        'CategoryName',
        'CreateDate',
        'ModifiedDate',
        'LocationID',
        'IsSync'
    ],
}


def get_db_connection():
    conn = pyodbc.connect(
        f"DRIVER={DB_DRIVER13};"
        f"SERVER={DB_SERVER};"
        f"DATABASE={DB_NAME};"
        f"UID={DB_USER};"
        f"PWD={DB_PASSWORD}"
    )
    return conn


def update_producer_status(producer_id, producer_name, location_id, last_heartbeat, status):
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("""
            MERGE INTO ProducerStatus AS target
            USING (VALUES (?, ?, ?, ?, ?)) AS source 
            (producer_id, producer_name, location_id, last_heartbeat, status)
            ON target.producer_id = source.producer_id

            WHEN MATCHED THEN
                UPDATE SET 
                    target.producer_name = source.producer_name,
                    target.location_id = source.location_id,
                    target.last_heartbeat = source.last_heartbeat,
                    target.status = source.status

            WHEN NOT MATCHED THEN
                INSERT (
                    producer_id, producer_name, location_id, last_heartbeat, status
                )
                VALUES (
                    source.producer_id, source.producer_name,
                    source.location_id, source.last_heartbeat, source.status
                );
        """, producer_id, producer_name, location_id, last_heartbeat, status)

        conn.commit()

    except pyodbc.Error as e:
        logger.error(f"Database error: {str(e)}")

    finally:
        cursor.close()
        conn.close()


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

            conn.commit()

        except pyodbc.Error as e:
            logger.error(f"Database error: {str(e)}")

        finally:
            cursor.close()
            conn.close()

        time.sleep(timeout)


def monitor_producer_heartbeat():
    consumer = KafkaConsumer(
        'producer_heartbeat',
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        enable_auto_commit=False,
        auto_offset_reset='latest',
        group_id='heartbeat_monitor',
        session_timeout_ms=30000,
        heartbeat_interval_ms=10000
    )

    for message in consumer:
        heartbeat_message = message.value

        producer_id = heartbeat_message['producer_id']
        producer_name = heartbeat_message['producer_name']
        location_id = heartbeat_message['location_id']
        timestamp_str = heartbeat_message['timestamp']

        last_heartbeat = datetime.fromisoformat(timestamp_str)
        status = 1

        logger.info(f"Received heartbeat message: {heartbeat_message}")

        update_producer_status(
            producer_id,
            producer_name,
            location_id,
            last_heartbeat,
            status
        )


def send_email(subject, body):
    msg = MIMEText(body)
    msg['Subject'] = subject
    msg['From'] = EMAIL_SENDER
    msg['To'] = EMAIL_RECEIVER

    try:
        with smtplib.SMTP(SMTP_SERVER, int(SMTP_PORT)) as server:
            server.starttls()
            server.login(EMAIL_SENDER, EMAIL_PASSWORD)
            server.send_message(msg)

        logger.info(f"Email sent: {subject}")

    except Exception as e:
        logger.error(f"Failed to send email: {e}")


def handle_interrupt(signum, frame):
    logger.info("Keyboard interrupt detected. Shutting down consumer2.")
    send_email(
        "Consumer2 Interrupted Alert",
        "The consumer2.py process has been interrupted. Please check the system status."
    )
    os._exit(0)


def handle_shutdown(signum, frame):
    logger.info("System is shutting down.")
    send_email(
        "Consumer2 Shutdown",
        "The system is shutting down. Check consumer2 status."
    )
    os._exit(0)


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
        "dbo.LogException",
        "dbo.Machine",
        "dbo.AppVersionLog",
        "dbo.MachineMapping",
        "dbo.MachineParameters",
        "dbo.CategoryMaster",
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        enable_auto_commit=False,
        auto_offset_reset='earliest',
        group_id='logexception_machine_sync',
        session_timeout_ms=30000,
        heartbeat_interval_ms=10000
    )

    logger.info("Consumer2 initialized. Starting message processing...")

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

            conn.commit()
            consumer.commit()

            logger.info(f"Processed {len(messages)} messages successfully")

        except Exception as e:
            logger.error(f"Error during processing: {str(e)}")
            conn.rollback()


def process_message(cursor, message):
    record = message.value
    table_name = message.topic

    columns = tables_columns.get(table_name)

    if not columns:
        logger.error(f"Unknown table: {table_name}")
        return False

    date_fields = [
        'ModifiedDate',
        'Timestamp',
        'CreateDate',
        'LogDate',
        'CreatedDate',
        'Modifieddate',
        'DateTimeofResult'
    ]

    for key in date_fields:
        if key in record and isinstance(record[key], str):
            try:
                record[key] = datetime.fromisoformat(record[key])
            except (ValueError, TypeError):
                record[key] = None

    primary_key = columns[0]

    try:
        # ============================
        # AppVersionLog Logic
        # ============================
        if table_name == "dbo.AppVersionLog":

            if record.get("Id") is None or record.get("CenterId") is None:
                logger.error("Missing Id or CenterId in dbo.AppVersionLog")
                return False

            cursor.execute(
                """
                SELECT 1 
                FROM dbo.AppVersionLog 
                WHERE Id = ? 
                  AND CenterId = ?
                """,
                (record["Id"], record["CenterId"])
            )

            if cursor.fetchone():
                query = """
                    UPDATE dbo.AppVersionLog
                    SET 
                        InstallationVersionNumber = ?,
                        InstallationSystemName = ?,
                        UserName = ?,
                        InstrumentName = ?,
                        LocationName = ?,
                        LogDate = ?,
                        BuildVersion = ?,
                        BuildDate = ?,
                        IsSync = 1
                    WHERE Id = ?
                      AND CenterId = ?
                """

                params = (
                    record.get("InstallationVersionNumber"),
                    record.get("InstallationSystemName"),
                    record.get("UserName"),
                    record.get("InstrumentName"),
                    record.get("LocationName"),
                    record.get("LogDate"),
                    record.get("BuildVersion"),
                    record.get("BuildDate"),
                    record["Id"],
                    record["CenterId"]
                )

            else:
                query = """
                    INSERT INTO dbo.AppVersionLog (
                        Id,
                        InstallationVersionNumber,
                        InstallationSystemName,
                        UserName,
                        InstrumentName,
                        LocationName,
                        CenterId,
                        LogDate,
                        BuildVersion,
                        BuildDate,
                        IsSync
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
                """

                params = (
                    record["Id"],
                    record.get("InstallationVersionNumber"),
                    record.get("InstallationSystemName"),
                    record.get("UserName"),
                    record.get("InstrumentName"),
                    record.get("LocationName"),
                    record["CenterId"],
                    record.get("LogDate"),
                    record.get("BuildVersion"),
                    record.get("BuildDate")
                )

            cursor.execute(query, params)
            return True

        # ============================
        # MachineMapping Logic
        # ============================
        if table_name == "dbo.MachineMapping":

            if record.get("MachineFId") is None or record.get("TestParamFID") is None or record.get("LocationID") is None:
                logger.error("Missing MachineFId/TestParamFID/LocationID in dbo.MachineMapping")
                return False

            if record.get("IsDeleted") == 1:
                logger.info(
                    f"Skipping deleted MachineMapping "
                    f"MachineFId={record.get('MachineFId')}, "
                    f"TestParamFID={record.get('TestParamFID')}, "
                    f"LocationID={record.get('LocationID')}"
                )
                return True

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
                query = f"""
                    INSERT INTO dbo.MachineMapping
                    ({', '.join(columns)})
                    VALUES ({', '.join(['?'] * len(columns))})
                """

                params = [record.get(col, None) for col in columns]

            cursor.execute(query, params)
            return True

        # ============================
        # MachineParameters Logic
        # ============================
        if table_name == "dbo.MachineParameters":

            if record.get("Id") is None or record.get("LocationID") is None:
                logger.error("Missing Id or LocationID in dbo.MachineParameters")
                return False

            cursor.execute(
                """
                SELECT 1
                FROM dbo.MachineParameters
                WHERE Id = ?
                  AND LocationID = ?
                """,
                (
                    record["Id"],
                    record["LocationID"]
                )
            )

            if cursor.fetchone():
                query = """
                    UPDATE dbo.MachineParameters
                    SET
                        Parameter = ?,
                        isDeleted = ?,
                        ParmeterHeader = ?,
                        MachineFID = ?,
                        ConfigMachineDataFID = ?,
                        ValueField = ?,
                        TestField = ?,
                        LISParamName = ?,
                        CreateDate = ?,
                        ModifiedDate = ?
                    WHERE Id = ?
                      AND LocationID = ?
                """

                params = (
                    record.get("Parameter"),
                    record.get("isDeleted"),
                    record.get("ParmeterHeader"),
                    record.get("MachineFID"),
                    record.get("ConfigMachineDataFID"),
                    record.get("ValueField"),
                    record.get("TestField"),
                    record.get("LISParamName"),
                    record.get("CreateDate"),
                    record.get("ModifiedDate"),
                    record["Id"],
                    record["LocationID"]
                )

            else:
                query = """
                    INSERT INTO dbo.MachineParameters (
                        Id,
                        Parameter,
                        isDeleted,
                        ParmeterHeader,
                        MachineFID,
                        ConfigMachineDataFID,
                        ValueField,
                        TestField,
                        LISParamName,
                        CreateDate,
                        ModifiedDate,
                        LocationID
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """

                params = (
                    record.get("Id"),
                    record.get("Parameter"),
                    record.get("isDeleted"),
                    record.get("ParmeterHeader"),
                    record.get("MachineFID"),
                    record.get("ConfigMachineDataFID"),
                    record.get("ValueField"),
                    record.get("TestField"),
                    record.get("LISParamName"),
                    record.get("CreateDate"),
                    record.get("ModifiedDate"),
                    record.get("LocationID")
                )

            cursor.execute(query, params)
            logger.info(
                f"MachineParameters synced successfully: "
                f"Id={record.get('Id')}, LocationID={record.get('LocationID')}"
            )
            return True

        # ============================
        # CategoryMaster Logic (Insert Only)
        # ============================
        if table_name == "dbo.CategoryMaster":

            if record.get("CategoryID") is None:
                logger.error("Missing CategoryID in dbo.CategoryMaster")
                return False

            cursor.execute(
                """
                SELECT 1
                FROM dbo.CategoryMaster
                WHERE CategoryID = ?
                """,
                (record["CategoryID"],)
            )

            if cursor.fetchone():
                logger.info(
                    f"CategoryMaster record already exists, skipping insert: "
                    f"CategoryID={record.get('CategoryID')}, "
                    f"CategoryName={record.get('CategoryName')}"
                )
                return True  # skip silently, no update

            query = """
                INSERT INTO dbo.CategoryMaster (
                    CategoryID,
                    CategoryName,
                    CreateDate,
                    ModifiedDate,
                    LocationID,
                    IsSync
                )
                VALUES (?, ?, ?, ?, ?, ?)
            """

            params = (
                record.get("CategoryID"),
                record.get("CategoryName"),
                record.get("CreateDate"),
                record.get("ModifiedDate"),
                record.get("LocationID"),
                record.get("IsSync")
            )

            cursor.execute(query, params)
            logger.info(
                f"CategoryMaster inserted successfully: "
                f"CategoryID={record.get('CategoryID')}, "
                f"CategoryName={record.get('CategoryName')}"
            )
            return True

        # ============================
        # Generic tables
        # ============================
        if record.get(primary_key) is None or record.get("LocationID") is None:
            logger.error(f"Missing {primary_key} or LocationID in {table_name}")
            return False

        location_id = record["LocationID"]

        cursor.execute(
            f"""
            SELECT 1 
            FROM {table_name} 
            WHERE {primary_key} = ? 
              AND LocationID = ?
            """,
            (record[primary_key], location_id)
        )

        if cursor.fetchone():
            update_cols = [
                f"{col} = ?"
                for col in columns
                if col != primary_key
            ]

            query = f"""
                UPDATE {table_name}
                SET {', '.join(update_cols)}
                WHERE {primary_key} = ?
                  AND LocationID = ?
            """

            params = (
                [record.get(col, None) for col in columns if col != primary_key]
                + [record[primary_key], location_id]
            )

        else:
            query = f"""
                INSERT INTO {table_name}
                ({', '.join(columns)})
                VALUES ({', '.join(['?'] * len(columns))})
            """

            params = [record.get(col, None) for col in columns]

        cursor.execute(query, params)
        return True

    except pyodbc.Error as e:
        logger.error(f"Database error in {table_name}: {str(e)}")
        raise

    except Exception as e:
        logger.error(f"Processing error in {table_name}: {str(e)}")
        return False


if __name__ == "__main__":
    signal.signal(signal.SIGINT, handle_interrupt)
    signal.signal(signal.SIGTERM, handle_shutdown)

    threading.Thread(
        target=monitor_producer_heartbeat,
        daemon=True
    ).start()

    threading.Thread(
        target=mark_producers_as_dead,
        daemon=True
    ).start()

    consume_messages()
