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
from email.mime.multipart import MIMEMultipart
from flask import Flask
from flask_cors import CORS
import threading
from dotenv import load_dotenv
from logging.handlers import TimedRotatingFileHandler


# ─── Load env variables ──────────────────────────────────────────────────────
load_dotenv()

# ─── Global variables ────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP          = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
PRODUCER_TIMEOUT_SECONDS = os.getenv("PRODUCER_TIMEOUT_SECONDS")

# Report times: 10:00 AM, 1:30 PM, 7:00 PM
REPORT_TIMES = ["10:00", "13:30", "19:00"]

# ─── Logging setup ───────────────────────────────────────────────────────────
LOG_DIR   = os.path.join(os.path.dirname(__file__), "logs")
os.makedirs(LOG_DIR, exist_ok=True)

LOG_LEVEL  = os.getenv("LOG_LEVEL", "INFO").upper()
LOG_FORMAT = (
    "%(asctime)s | %(levelname)s | %(process)d | "
    "%(threadName)s | %(filename)s:%(lineno)d | %(message)s"
)

formatter = logging.Formatter(LOG_FORMAT)

file_handler = TimedRotatingFileHandler(
    filename=os.path.join(LOG_DIR, "consumer2.log"),
    when="H", interval=12, backupCount=24, encoding="utf-8"
)
file_handler.setFormatter(formatter)
file_handler.setLevel(LOG_LEVEL)

error_handler = TimedRotatingFileHandler(
    filename=os.path.join(LOG_DIR, "consumer2_error.log"),
    when="H", interval=12, backupCount=24, encoding="utf-8"
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

# ─── Email configuration ─────────────────────────────────────────────────────
EMAIL_SENDER   = os.getenv("EMAIL_SENDER")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
EMAIL_RECEIVER = "kaustubhwandile@gmail.com"
SMTP_SERVER    = os.getenv("SMTP_SERVER")
SMTP_PORT      = os.getenv("SMTP_PORT")

# ─── Database configuration ──────────────────────────────────────────────────
DB_DRIVER13  = os.getenv("DB_DRIVER13")
DB_SERVER    = os.getenv("DB_SERVER")
DB_NAME      = os.getenv("DB_NAME")
DB_USER      = os.getenv("DB_USER")
DB_PASSWORD  = os.getenv("DB_PASSWORD")

timeout = 60

# ─── Flask app ───────────────────────────────────────────────────────────────
app = Flask(__name__)
CORS(app)

# ─── Table columns map ───────────────────────────────────────────────────────
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
        'Id', 'Parameter', 'isDeleted',
        'ParmeterHeader', 'MachineFID',
        'ConfigMachineDataFID', 'ValueField',
        'TestField', 'LISParamName',
        'CreateDate', 'ModifiedDate', 'LocationID'
    ],
    "ConfigMachineData": [
        'ConfigMachineDataId', 'FormatDetails',
        'MachinFID', 'TestName', 'TestFID',
        'ResultMatchField', 'MachineResultParam',
        'MachineResultSegment', 'pFrom', 'pTo',
        'OrderMatchField', 'MachineOrdertParam',
        'MachineOrderSegment', 'OrderLevel',
        'OrderFieldName', 'CategoryFID',
        'TimeInterval', 'Querymode',
        'SubResultSegment', 'CreateDate',
        'ModifiedDate', 'LocationID', 'IsSync'
    ],
    "dbo.CategoryMaster": [
        'CategoryID', 'CategoryName',
        'CreateDate', 'ModifiedDate',
        'LocationID', 'IsSync'
    ],
}


# ════════════════════════════════════════════════════════════════════════════
#  DATABASE HELPERS
# ════════════════════════════════════════════════════════════════════════════

def get_db_connection():
    return pyodbc.connect(
        f"DRIVER={DB_DRIVER13};"
        f"SERVER={DB_SERVER};"
        f"DATABASE={DB_NAME};"
        f"UID={DB_USER};"
        f"PWD={DB_PASSWORD}"
    )


# ════════════════════════════════════════════════════════════════════════════
#  MACHINE STATUS QUERIES  (uses dbo.LogException for machine status)
# ════════════════════════════════════════════════════════════════════════════

def get_machine_status_data():

    """
    Returns machine status using dbo.LogException.
    A machine is considered ONLINE  if its latest LogException has LogType = 'Connected'
                             OFFLINE if its latest LogException has LogType = 'Disconnected'
                                     OR if it has no log entry at all.

    Returns:
        total          (int)
        total_online   (int)
        total_offline  (int)
        location_stats (list of dicts)  — per location breakdown
        disconnected   (list of dicts)  — offline machine details
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        # ── Latest log entry per machine ──────────────────────────────────
        # We grab each machine, join to the most recent LogException row,
        # and decide online/offline from LogType.
        cursor.execute("""
            SELECT
                M.ID                                        AS MachineID,
                M.MachineName,
                M.LocationID,
                ISNULL(L.LocationName, 'Unknown')           AS LocationName,
                LE.LogType,
                LE.Timestamp                                AS LastLogTime
            FROM dbo.Machine M
            LEFT JOIN dbo.Location L
                ON L.LocationID = M.LocationID
            LEFT JOIN (
                SELECT MachineFID,
                       LogType,
                       Timestamp,
                       ROW_NUMBER() OVER (
                           PARTITION BY MachineFID
                           ORDER BY Timestamp DESC
                       ) AS rn
                FROM dbo.LogException
            ) LE ON LE.MachineFID = M.ID AND LE.rn = 1
            WHERE M.IsDeleted = 0
            ORDER BY LocationName, M.MachineName
        """)

        rows = cursor.fetchall()

        all_machines    = []
        disconnected    = []
        location_map    = {}   # LocationID → {name, total, online, offline}

        for row in rows:
            machine_id    = row[0]
            machine_name  = row[1]
            location_id   = row[2]
            location_name = row[3]
            log_type      = row[4]   # 'Connected' / 'Disconnected' / None
            last_log_time = row[5]

            # Determine status
            is_online = (log_type is not None and
                         log_type.strip().lower() == 'connected')

            machine = {
                "machine_id":    machine_id,
                "machine_name":  machine_name,
                "location_id":   location_id,
                "location_name": location_name,
                "status":        "Online" if is_online else "Offline",
                "last_log_time": last_log_time.strftime("%d-%m-%Y %H:%M:%S")
                                  if last_log_time else "No log found",
                "log_type":      log_type or "No log"
            }
            all_machines.append(machine)

            if not is_online:
                disconnected.append(machine)

            # Build per-location aggregation
            if location_id not in location_map:
                location_map[location_id] = {
                    "location_name": location_name,
                    "total":   0,
                    "online":  0,
                    "offline": 0
                }
            location_map[location_id]["total"]  += 1
            if is_online:
                location_map[location_id]["online"]  += 1
            else:
                location_map[location_id]["offline"] += 1

        location_stats = sorted(
            location_map.values(),
            key=lambda x: x["total"],
            reverse=True
        )

        total         = len(all_machines)
        total_online  = sum(1 for m in all_machines if m["status"] == "Online")
        total_offline = total - total_online

        return total, total_online, total_offline, location_stats, disconnected

    except pyodbc.Error as e:
        logger.error(f"Error fetching machine status data: {e}")
        return 0, 0, 0, [], []
    finally:
        cursor.close()
        conn.close()


# ════════════════════════════════════════════════════════════════════════════
#  EMAIL BUILDER
# ════════════════════════════════════════════════════════════════════════════

def build_machine_status_email():
    """Build full HTML email with machine status report."""

    total, total_online, total_offline, location_stats, disconnected = get_machine_status_data()
    now_str   = datetime.now().strftime("%d %b %Y %H:%M")
    gen_str   = datetime.now().strftime("%d-%m-%Y %H:%M:%S")

    # Determine report slot label
    hour = datetime.now().hour
    if hour < 12:
        slot_label = "Morning Report"
    elif hour < 16:
        slot_label = "Afternoon Report"
    else:
        slot_label = "Evening Report"

    # ── Location-wise summary table rows ─────────────────────────────────
    location_rows_html = ""
    for idx, loc in enumerate(location_stats, start=1):
        bg = "#f7faff" if idx % 2 == 0 else "#ffffff"
        online_color  = "#1e8a2e" if loc["online"]  > 0 else "#999"
        offline_color = "#cc0000" if loc["offline"] > 0 else "#999"
        location_rows_html += f"""
        <tr style="background:{bg};">
            <td style="padding:9px 14px; border:1px solid #e0e6f0; text-align:center;">{idx}</td>
            <td style="padding:9px 14px; border:1px solid #e0e6f0;">{loc['location_name']}</td>
            <td style="padding:9px 14px; border:1px solid #e0e6f0; text-align:center;">
                <strong>{loc['total']}</strong>
            </td>
            <td style="padding:9px 14px; border:1px solid #e0e6f0; text-align:center;
                       color:{online_color}; font-weight:bold;">
                🟢 {loc['online']}
            </td>
            <td style="padding:9px 14px; border:1px solid #e0e6f0; text-align:center;
                       color:{offline_color}; font-weight:bold;">
                🔴 {loc['offline']}
            </td>
        </tr>"""

    # Grand total footer row
    location_rows_html += f"""
        <tr style="background:#1a3c78; color:#fff;">
            <td style="padding:10px 14px; border:1px solid #2c5f9e;" colspan="2">
                <strong>Grand Total</strong>
            </td>
            <td style="padding:10px 14px; border:1px solid #2c5f9e; text-align:center;">
                <strong>{total}</strong>
            </td>
            <td style="padding:10px 14px; border:1px solid #2c5f9e; text-align:center;">
                <strong>🟢 {total_online}</strong>
            </td>
            <td style="padding:10px 14px; border:1px solid #2c5f9e; text-align:center;">
                <strong>🔴 {total_offline}</strong>
            </td>
        </tr>"""

    # ── Disconnected machines detail table ───────────────────────────────
    if disconnected:
        disconnected_rows_html = ""
        for idx, m in enumerate(disconnected, start=1):
            bg = "#fff5f5" if idx % 2 == 0 else "#ffffff"
            disconnected_rows_html += f"""
            <tr style="background:{bg};">
                <td style="padding:9px 14px; border:1px solid #f0d0d0; text-align:center;">{idx}</td>
                <td style="padding:9px 14px; border:1px solid #f0d0d0; text-align:center;">
                    {m['machine_id']}
                </td>
                <td style="padding:9px 14px; border:1px solid #f0d0d0;">
                    <strong>{m['machine_name']}</strong>
                </td>
                <td style="padding:9px 14px; border:1px solid #f0d0d0;">
                    {m['location_name']}
                </td>
                <td style="padding:9px 14px; border:1px solid #f0d0d0; text-align:center;">
                    {m['location_id']}
                </td>
                <td style="padding:9px 14px; border:1px solid #f0d0d0; color:#888; font-size:12px;">
                    {m['last_log_time']}
                </td>
            </tr>"""

        disconnected_section = f"""
        <h3 style="color:#cc0000; margin-top:32px; font-family:Arial,sans-serif;">
            🔴 Disconnected Machines — {total_offline} machine(s) offline
        </h3>
        <table style="border-collapse:collapse; width:100%; max-width:750px; font-family:Arial,sans-serif; font-size:13px;">
            <thead>
                <tr style="background:#cc0000; color:#fff;">
                    <th style="padding:10px 14px; border:1px solid #f0d0d0;">#</th>
                    <th style="padding:10px 14px; border:1px solid #f0d0d0;">Machine ID</th>
                    <th style="padding:10px 14px; border:1px solid #f0d0d0;">Machine Name</th>
                    <th style="padding:10px 14px; border:1px solid #f0d0d0;">Location Name</th>
                    <th style="padding:10px 14px; border:1px solid #f0d0d0;">Location ID</th>
                    <th style="padding:10px 14px; border:1px solid #f0d0d0;">Last Log Time</th>
                </tr>
            </thead>
            <tbody>
                {disconnected_rows_html}
            </tbody>
        </table>"""
    else:
        disconnected_section = """
        <div style="margin-top:28px; padding:16px 24px; background:#e6f4ea;
                    border-left:5px solid #1e8a2e; border-radius:4px;
                    font-family:Arial,sans-serif;">
            <strong style="color:#1e8a2e; font-size:15px;">
                ✅ All machines are currently Online!
            </strong>
        </div>"""

    # ── Full HTML body ────────────────────────────────────────────────────
    body = f"""
    <!DOCTYPE html>
    <html>
    <body style="margin:0; padding:24px; background:#f0f4f8;
                 font-family:Arial,sans-serif; color:#333;">

        <!-- Header -->
        <div style="background:#1a3c78; padding:20px 28px; border-radius:8px 8px 0 0;">
            <h2 style="margin:0; color:#fff; font-size:20px;">
                📊 Machine Status Report &nbsp;·&nbsp; {slot_label}
            </h2>
            <p style="margin:4px 0 0; color:#a8c4f0; font-size:13px;">{now_str}</p>
        </div>

        <!-- Content card -->
        <div style="background:#fff; padding:28px; border-radius:0 0 8px 8px;
                    box-shadow:0 2px 8px rgba(0,0,0,0.08);">

            <!-- Summary cards -->
            <table style="border-collapse:separate; border-spacing:14px; margin-bottom:8px;">
                <tr>
                    <td style="background:#e8f0fe; padding:16px 32px; border-radius:8px;
                               text-align:center; min-width:100px;">
                        <div style="font-size:12px; color:#555; margin-bottom:4px;">
                            Total Machines
                        </div>
                        <div style="font-size:32px; font-weight:bold; color:#1a3c78;">
                            {total}
                        </div>
                    </td>
                    <td style="background:#e6f4ea; padding:16px 32px; border-radius:8px;
                               text-align:center; min-width:100px;">
                        <div style="font-size:12px; color:#555; margin-bottom:4px;">
                            🟢 Online
                        </div>
                        <div style="font-size:32px; font-weight:bold; color:#1e8a2e;">
                            {total_online}
                        </div>
                    </td>
                    <td style="background:#fce8e6; padding:16px 32px; border-radius:8px;
                               text-align:center; min-width:100px;">
                        <div style="font-size:12px; color:#555; margin-bottom:4px;">
                            🔴 Offline
                        </div>
                        <div style="font-size:32px; font-weight:bold; color:#cc0000;">
                            {total_offline}
                        </div>
                    </td>
                </tr>
            </table>

            <!-- Location-wise breakdown -->
            <h3 style="margin-top:28px; margin-bottom:10px; color:#1a3c78;">
                📍 Location-wise Breakdown
            </h3>
            <table style="border-collapse:collapse; width:100%; max-width:650px; font-size:13px;">
                <thead>
                    <tr style="background:#2c5f9e; color:#fff;">
                        <th style="padding:10px 14px; border:1px solid #4a7bbf;">#</th>
                        <th style="padding:10px 14px; border:1px solid #4a7bbf; text-align:left;">
                            Location Name
                        </th>
                        <th style="padding:10px 14px; border:1px solid #4a7bbf;">Total</th>
                        <th style="padding:10px 14px; border:1px solid #4a7bbf;">Online</th>
                        <th style="padding:10px 14px; border:1px solid #4a7bbf;">Offline</th>
                    </tr>
                </thead>
                <tbody>
                    {location_rows_html}
                </tbody>
            </table>

            <!-- Disconnected machines detail -->
            {disconnected_section}

            <!-- Footer -->
            <p style="margin-top:36px; color:#bbb; font-size:11px; border-top:1px solid #eee;
                      padding-top:12px;">
                Generated at {gen_str} &nbsp;|&nbsp; Consumer2 Machine Monitor
                &nbsp;|&nbsp; Report Schedule: 10:00 AM · 1:30 PM · 7:00 PM
            </p>

        </div>
    </body>
    </html>
    """
    return body


# ════════════════════════════════════════════════════════════════════════════
#  EMAIL SENDERS
# ════════════════════════════════════════════════════════════════════════════

def send_email(subject, body):
    """Plain-text email (used for interrupt/shutdown alerts)."""
    msg = MIMEText(body)
    msg['Subject'] = subject
    msg['From']    = EMAIL_SENDER
    msg['To']      = EMAIL_RECEIVER
    try:
        with smtplib.SMTP(SMTP_SERVER, int(SMTP_PORT)) as server:
            server.starttls()
            server.login(EMAIL_SENDER, EMAIL_PASSWORD)
            server.send_message(msg)
        logger.info(f"Email sent: {subject}")
    except Exception as e:
        logger.error(f"Failed to send email: {e}")


def send_html_email(subject, html_body):
    """HTML email for machine status reports."""
    msg = MIMEMultipart("alternative")
    msg['Subject'] = subject
    msg['From']    = EMAIL_SENDER
    msg['To']      = EMAIL_RECEIVER
    msg.attach(MIMEText(html_body, "html"))
    try:
        with smtplib.SMTP(SMTP_SERVER, int(SMTP_PORT)) as server:
            server.starttls()
            server.login(EMAIL_SENDER, EMAIL_PASSWORD)
            server.send_message(msg)
        logger.info(f"HTML report email sent: {subject}")
    except Exception as e:
        logger.error(f"Failed to send HTML email: {e}")


# ════════════════════════════════════════════════════════════════════════════
#  SCHEDULED REPORT  (10:00 AM · 1:30 PM · 7:00 PM)
# ════════════════════════════════════════════════════════════════════════════

def schedule_machine_report():
    """
    Background thread — fires machine status email at:
        10:00 AM, 1:30 PM, 7:00 PM every day.
    """
    logger.info(
        f"Machine report scheduler started. "
        f"Send times: {', '.join(REPORT_TIMES)}"
    )

    while True:
        now = datetime.now()

        # Build today's scheduled datetime objects
        todays_slots = []
        for t in REPORT_TIMES:
            hour, minute = map(int, t.split(":"))
            slot = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
            todays_slots.append(slot)

        # Find the next upcoming slot
        future_slots = [s for s in todays_slots if s > now]

        if future_slots:
            next_run = min(future_slots)
        else:
            # All done for today → schedule first slot tomorrow
            hour, minute = map(int, REPORT_TIMES[0].split(":"))
            next_run = (now + timedelta(days=1)).replace(
                hour=hour, minute=minute, second=0, microsecond=0
            )

        wait_seconds = (next_run - now).total_seconds()
        logger.info(
            f"Next machine report at {next_run.strftime('%d-%m-%Y %H:%M')} "
            f"({wait_seconds / 3600:.2f} hrs away)"
        )

        time.sleep(wait_seconds)

        try:
            now_label = datetime.now().strftime("%d %b %Y %H:%M")
            subject   = f"Machine Status Report — {now_label}"
            body      = build_machine_status_email()
            send_html_email(subject, body)
            logger.info(f"Machine status report sent at {now_label}")
        except Exception as e:
            logger.error(f"Machine report send failed: {e}")

        # Small buffer so we don't re-trigger the same slot
        time.sleep(65)


# ════════════════════════════════════════════════════════════════════════════
#  PRODUCER HEARTBEAT MONITOR
# ════════════════════════════════════════════════════════════════════════════

def update_producer_status(producer_id, producer_name, location_id, last_heartbeat, status):
    conn   = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            MERGE INTO ProducerStatus AS target
            USING (VALUES (?, ?, ?, ?, ?)) AS source
            (producer_id, producer_name, location_id, last_heartbeat, status)
            ON target.producer_id = source.producer_id
            WHEN MATCHED THEN
                UPDATE SET
                    target.producer_name   = source.producer_name,
                    target.location_id     = source.location_id,
                    target.last_heartbeat  = source.last_heartbeat,
                    target.status          = source.status
            WHEN NOT MATCHED THEN
                INSERT (producer_id, producer_name, location_id, last_heartbeat, status)
                VALUES (source.producer_id, source.producer_name,
                        source.location_id, source.last_heartbeat, source.status);
        """, producer_id, producer_name, location_id, last_heartbeat, status)
        conn.commit()
    except pyodbc.Error as e:
        logger.error(f"Database error in update_producer_status: {e}")
    finally:
        cursor.close()
        conn.close()


def mark_producers_as_dead():
    while True:
        conn   = get_db_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("""
                UPDATE ProducerStatus
                SET status = 0
                WHERE last_heartbeat < ?
            """, datetime.now() - timedelta(seconds=timeout))
            conn.commit()
        except pyodbc.Error as e:
            logger.error(f"Database error in mark_producers_as_dead: {e}")
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
        hb            = message.value
        producer_id   = hb['producer_id']
        producer_name = hb['producer_name']
        location_id   = hb['location_id']
        last_heartbeat = datetime.fromisoformat(hb['timestamp'])

        logger.info(f"Received heartbeat: {hb}")

        update_producer_status(producer_id, producer_name, location_id, last_heartbeat, 1)


# ════════════════════════════════════════════════════════════════════════════
#  SIGNAL HANDLERS
# ════════════════════════════════════════════════════════════════════════════

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
        "Consumer2 Shutdown Alert",
        "The system is shutting down. Check consumer2 status."
    )
    os._exit(0)


# ════════════════════════════════════════════════════════════════════════════
#  KAFKA CONSUMER — MESSAGE PROCESSING
# ════════════════════════════════════════════════════════════════════════════

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
            logger.error(f"Error during processing: {e}")
            conn.rollback()


def process_message(cursor, message):
    record     = message.value
    table_name = message.topic
    columns    = tables_columns.get(table_name)

    if not columns:
        logger.error(f"Unknown table: {table_name}")
        return False

    date_fields = [
        'ModifiedDate', 'Timestamp', 'CreateDate',
        'LogDate', 'CreatedDate', 'Modifieddate', 'DateTimeofResult'
    ]

    for key in date_fields:
        if key in record and isinstance(record[key], str):
            try:
                record[key] = datetime.fromisoformat(record[key])
            except (ValueError, TypeError):
                record[key] = None

    primary_key = columns[0]

    try:
        # ── AppVersionLog ─────────────────────────────────────────────────
        if table_name == "dbo.AppVersionLog":
            if record.get("Id") is None or record.get("CenterId") is None:
                logger.error("Missing Id or CenterId in dbo.AppVersionLog")
                return False

            cursor.execute(
                "SELECT 1 FROM dbo.AppVersionLog WHERE Id=? AND CenterId=?",
                (record["Id"], record["CenterId"])
            )

            if cursor.fetchone():
                cursor.execute("""
                    UPDATE dbo.AppVersionLog
                    SET InstallationVersionNumber=?, InstallationSystemName=?,
                        UserName=?, InstrumentName=?, LocationName=?,
                        LogDate=?, BuildVersion=?, BuildDate=?, IsSync=1
                    WHERE Id=? AND CenterId=?
                """, (
                    record.get("InstallationVersionNumber"),
                    record.get("InstallationSystemName"),
                    record.get("UserName"),
                    record.get("InstrumentName"),
                    record.get("LocationName"),
                    record.get("LogDate"),
                    record.get("BuildVersion"),
                    record.get("BuildDate"),
                    record["Id"], record["CenterId"]
                ))
            else:
                cursor.execute("""
                    INSERT INTO dbo.AppVersionLog
                    (Id,InstallationVersionNumber,InstallationSystemName,UserName,
                     InstrumentName,LocationName,CenterId,LogDate,BuildVersion,BuildDate,IsSync)
                    VALUES (?,?,?,?,?,?,?,?,?,?,1)
                """, (
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
                ))
            return True

        # ── MachineMapping ────────────────────────────────────────────────
        if table_name == "dbo.MachineMapping":
            if (record.get("MachineFId") is None or
                    record.get("TestParamFID") is None or
                    record.get("LocationID") is None):
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
                """SELECT 1 FROM dbo.MachineMapping
                   WHERE MachineFId=? AND TestParamFID=? AND LocationID=?""",
                (record["MachineFId"], record["TestParamFID"], record["LocationID"])
            )

            if cursor.fetchone():
                update_cols = [
                    f"{col} = ?"
                    for col in columns
                    if col not in ("Id", "CreateDate")
                ]
                query  = f"""
                    UPDATE dbo.MachineMapping
                    SET {', '.join(update_cols)}
                    WHERE MachineFId=? AND TestParamFID=? AND LocationID=?
                """
                params = (
                    [record.get(col) for col in columns if col not in ("Id", "CreateDate")]
                    + [record["MachineFId"], record["TestParamFID"], record["LocationID"]]
                )
            else:
                query  = f"""
                    INSERT INTO dbo.MachineMapping ({', '.join(columns)})
                    VALUES ({', '.join(['?'] * len(columns))})
                """
                params = [record.get(col) for col in columns]

            cursor.execute(query, params)
            return True

        # ── MachineParameters ─────────────────────────────────────────────
        if table_name == "dbo.MachineParameters":
            if record.get("Id") is None or record.get("LocationID") is None:
                logger.error("Missing Id or LocationID in dbo.MachineParameters")
                return False

            cursor.execute(
                "SELECT 1 FROM dbo.MachineParameters WHERE Id=? AND LocationID=?",
                (record["Id"], record["LocationID"])
            )

            if cursor.fetchone():
                cursor.execute("""
                    UPDATE dbo.MachineParameters
                    SET Parameter=?, isDeleted=?, ParmeterHeader=?, MachineFID=?,
                        ConfigMachineDataFID=?, ValueField=?, TestField=?,
                        LISParamName=?, CreateDate=?, ModifiedDate=?
                    WHERE Id=? AND LocationID=?
                """, (
                    record.get("Parameter"), record.get("isDeleted"),
                    record.get("ParmeterHeader"), record.get("MachineFID"),
                    record.get("ConfigMachineDataFID"), record.get("ValueField"),
                    record.get("TestField"), record.get("LISParamName"),
                    record.get("CreateDate"), record.get("ModifiedDate"),
                    record["Id"], record["LocationID"]
                ))
            else:
                cursor.execute("""
                    INSERT INTO dbo.MachineParameters
                    (Id,Parameter,isDeleted,ParmeterHeader,MachineFID,
                     ConfigMachineDataFID,ValueField,TestField,LISParamName,
                     CreateDate,ModifiedDate,LocationID)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                """, (
                    record.get("Id"), record.get("Parameter"),
                    record.get("isDeleted"), record.get("ParmeterHeader"),
                    record.get("MachineFID"), record.get("ConfigMachineDataFID"),
                    record.get("ValueField"), record.get("TestField"),
                    record.get("LISParamName"), record.get("CreateDate"),
                    record.get("ModifiedDate"), record.get("LocationID")
                ))

            logger.info(
                f"MachineParameters synced: "
                f"Id={record.get('Id')}, LocationID={record.get('LocationID')}"
            )
            return True

        # ── CategoryMaster (insert only) ──────────────────────────────────
        if table_name == "dbo.CategoryMaster":
            if record.get("CategoryID") is None or record.get("LocationID") is None:
                logger.error("Missing CategoryID or LocationID in dbo.CategoryMaster")
                return False

            cursor.execute(
                "SELECT 1 FROM dbo.CategoryMaster WHERE CategoryID=? AND LocationID=?",
                (record["CategoryID"], record["LocationID"])
            )

            if cursor.fetchone():
                logger.info(
                    f"CategoryMaster already exists: "
                    f"CategoryID={record.get('CategoryID')}, "
                    f"LocationID={record.get('LocationID')}"
                )
                return True

            cursor.execute("""
                INSERT INTO dbo.CategoryMaster
                (CategoryID,CategoryName,CreateDate,ModifiedDate,LocationID,IsSync)
                VALUES (?,?,?,?,?,1)
            """, (
                record.get("CategoryID"), record.get("CategoryName"),
                record.get("CreateDate"), record.get("ModifiedDate"),
                record.get("LocationID")
            ))

            logger.info(
                f"CategoryMaster inserted: "
                f"CategoryID={record.get('CategoryID')}, "
                f"CategoryName={record.get('CategoryName')}"
            )
            return True

        # ── Generic tables ────────────────────────────────────────────────
        if record.get(primary_key) is None or record.get("LocationID") is None:
            logger.error(f"Missing {primary_key} or LocationID in {table_name}")
            return False

        location_id = record["LocationID"]

        cursor.execute(
            f"SELECT 1 FROM {table_name} WHERE {primary_key}=? AND LocationID=?",
            (record[primary_key], location_id)
        )

        if cursor.fetchone():
            update_cols = [f"{col} = ?" for col in columns if col != primary_key]
            query  = f"""
                UPDATE {table_name}
                SET {', '.join(update_cols)}
                WHERE {primary_key}=? AND LocationID=?
            """
            params = (
                [record.get(col) for col in columns if col != primary_key]
                + [record[primary_key], location_id]
            )
        else:
            query  = f"""
                INSERT INTO {table_name} ({', '.join(columns)})
                VALUES ({', '.join(['?'] * len(columns))})
            """
            params = [record.get(col) for col in columns]

        cursor.execute(query, params)
        return True

    except pyodbc.Error as e:
        logger.error(f"Database error in {table_name}: {e}")
        raise
    except Exception as e:
        logger.error(f"Processing error in {table_name}: {e}")
        return False


# ════════════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    signal.signal(signal.SIGINT,  handle_interrupt)
    signal.signal(signal.SIGTERM, handle_shutdown)

    # Heartbeat monitor
    threading.Thread(
        target=monitor_producer_heartbeat,
        name="HeartbeatMonitor",
        daemon=True
    ).start()

    # Mark dead producers
    threading.Thread(
        target=mark_producers_as_dead,
        name="DeadProducerChecker",
        daemon=True
    ).start()

    # Machine status report — 10:00 AM, 1:30 PM, 7:00 PM
    threading.Thread(
        target=schedule_machine_report,
        name="MachineReportScheduler",
        daemon=True
    ).start()

    logger.info("All threads started. Running consume_messages...")
    consume_messages()