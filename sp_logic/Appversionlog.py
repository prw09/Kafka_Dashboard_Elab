import json
import pyodbc
from kafka import KafkaConsumer
from datetime import datetime

# --------------------------------------------------
# Database Connection
# --------------------------------------------------
conn = pyodbc.connect(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=localhost;"
    "DATABASE=PathKind_BPHT;"
    "UID=sa;"
    "PWD=your_password"
)
cursor = conn.cursor()

# --------------------------------------------------
# Kafka Consumer
# --------------------------------------------------
consumer = KafkaConsumer(
    "app_version_topic",
    bootstrap_servers=["localhost:9092"],
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    auto_offset_reset="latest",
    enable_auto_commit=True,
    group_id="app_version_etl"
)

# --------------------------------------------------
# Helper: Get Location & Center
# --------------------------------------------------
def get_location_center(instrument_name):
    query = """
        SELECT lm.LocationName, lm.CenterId
        FROM Machine m
        INNER JOIN LocationMaster lm
            ON m.LocationID = lm.CenterId
        WHERE m.MachineName = ?
    """
    cursor.execute(query, instrument_name)
    row = cursor.fetchone()

    if row:
        return row.LocationName, row.CenterId
    return "Unknown", 0

# --------------------------------------------------
# ETL Processing
# --------------------------------------------------
for message in consumer:
    data = message.value

    version = data["version"]
    system_name = data["system_name"]
    user_name = data["user_name"]
    instrument_name = data["instrument_name"]
    build_version = data["build_version"]
    build_date = data["build_date"]

    # 1️⃣ Get Location & Center
    location_name, center_id = get_location_center(instrument_name)

    # 2️⃣ Insert into InsertVersionLog (History)
    cursor.execute("""
        SELECT 1 FROM InsertVersionLog
        WHERE InstallationVersionNumber = ?
          AND InstrumentName = ?
          AND CenterId = ?
          AND BuildVersion = ?
          AND BuildDate = ?
    """, version, instrument_name, center_id, build_version, build_date)

    if not cursor.fetchone():
        cursor.execute("""
            INSERT INTO InsertVersionLog (
                InstallationVersionNumber,
                InstallationSystemName,
                UserName,
                InstrumentName,
                LocationName,
                CenterId,
                LogDate,
                BuildVersion,
                BuildDate
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            version,
            system_name,
            user_name,
            instrument_name,
            location_name,
            center_id,
            datetime.now(),
            build_version,
            build_date
        ))

    # 3️⃣ UPSERT AppVersionLog (Main Table)
    cursor.execute("""
        SELECT 1 FROM AppVersionLog
        WHERE InstrumentName = ?
          AND CenterId = ?
    """, instrument_name, center_id)

    if cursor.fetchone():
        # UPDATE
        cursor.execute("""
            UPDATE AppVersionLog
            SET InstallationVersionNumber = ?,
                InstallationSystemName = ?,
                UserName = ?,
                LocationName = ?,
                LogDate = ?,
                BuildVersion = ?,
                BuildDate = ?,
                IsSync = 0
            WHERE InstrumentName = ?
              AND CenterId = ?
        """, (
            version,
            system_name,
            user_name,
            location_name,
            datetime.now(),
            build_version,
            build_date,
            instrument_name,
            center_id
        ))
    else:
        # INSERT
        cursor.execute("""
            INSERT INTO AppVersionLog (
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
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
        """, (
            version,
            system_name,
            user_name,
            instrument_name,
            location_name,
            center_id,
            datetime.now(),
            build_version,
            build_date
        ))

    conn.commit()
