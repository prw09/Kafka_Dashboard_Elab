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

# Report times: 10:00 AM daily
REPORT_TIMES = ["10:00"]


# ─── Logging setup ───────────────────────────────────────────────────────────
LOG_DIR = os.path.join(os.path.dirname(__file__), "logs")
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
EMAIL_SENDER        = os.getenv("EMAIL_SENDER")         # defaulterdyp@gmail.com
EMAIL_PASSWORD      = os.getenv("EMAIL_PASSWORD")       # Gmail App Password
EMAIL_RECEIVER_LIST = [
    e.strip()
    for e in os.getenv("EMAIL_RECEIVER", "").split(",")
    if e.strip()
]
SMTP_SERVER         = os.getenv("SMTP_SERVER")          # smtp.gmail.com
SMTP_PORT           = os.getenv("SMTP_PORT")            # 587

# ─── Database configuration ──────────────────────────────────────────────────
DB_DRIVER13  = os.getenv("DB_DRIVER13")     # ODBC Driver 13 for SQL Server
DB_SERVER    = os.getenv("DB_SERVER")       # GGNELAB
DB_NAME      = os.getenv("DB_NAME")         # Central_Pathkind_BPHT
DB_USER      = os.getenv("DB_USER")         # sa
DB_PASSWORD  = os.getenv("DB_PASSWORD")     # Labsoul@2024

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
#  MACHINE STATUS QUERIES  (uses dbo.GetMachineInformationForEmail SP)
# ════════════════════════════════════════════════════════════════════════════

def get_machine_status_data():
    """
    Calls dbo.GetMachineInformationForEmail:
        @TaskID = 1  → Total / Online / Offline counts
        @TaskID = 2  → Offline machine list with location, name, etc.

    Returns:
        total          (int)
        total_online   (int)
        total_offline  (int)
        location_stats (list of dicts)  — per-location breakdown built from offline list
        disconnected   (list of dicts)  — offline machine details
    """

    conn = get_db_connection()
    try:
        # ── TaskID = 1 : Summary counts ───────────────────────────────────
        cursor1 = conn.cursor()
        cursor1.execute("EXEC dbo.GetMachineInformationForEmail @TaskID = 1")
        row           = cursor1.fetchone()
        total         = int(row[0]) if row else 0
        total_online  = int(row[1]) if row else 0
        total_offline = int(row[2]) if row else 0
        cursor1.close()

        # ── TaskID = 2 : Offline machine list ─────────────────────────────
        cursor2 = conn.cursor()
        cursor2.execute("EXEC dbo.GetMachineInformationForEmail @TaskID = 2")
        rows = cursor2.fetchall()
        cursor2.close()

        disconnected = []
        location_map = {}

        for r in rows:
            machine_name    = r[0]
            location_name   = r[1]
            category_name   = r[2]
            connection_mode = r[3]
            qc_status       = r[4]
            machine_id      = r[5]
            location_id     = r[6]
            indicator       = r[7]
            troubleshoot    = r[8]  or ""
            message_string  = r[9]  or ""
            log_type        = r[10]
            build_version   = r[11] or "N/A"
            build_date      = r[12]

            machine = {
                "machine_id":      machine_id,
                "machine_name":    machine_name    or "Unknown",
                "location_id":     location_id,
                "location_name":   location_name   or "Unknown",
                "category_name":   category_name   or "N/A",
                "connection_mode": connection_mode or "N/A",
                "qc_status":       qc_status       or "N/A",
                "troubleshoot":    troubleshoot,
                "message_string":  message_string,
                "log_type":        log_type,
                "build_version":   build_version,
                "build_date": (
                    build_date.strftime("%d-%m-%Y")
                    if build_date and hasattr(build_date, 'strftime')
                    else (build_date if build_date else "N/A")
                ),
            }
            disconnected.append(machine)

            if location_id not in location_map:
                location_map[location_id] = {
                    "location_name": location_name or "Unknown",
                    "machines":      []
                }
            location_map[location_id]["machines"].append(machine)

        location_stats = sorted(
            [
                {
                    "location_id":   lid,
                    "location_name": v["location_name"],
                    "offline_count": len(v["machines"]),
                    "machines":      v["machines"],
                }
                for lid, v in location_map.items()
            ],
            key=lambda x: x["location_name"]
        )

        return total, total_online, total_offline, location_stats, disconnected

    except pyodbc.Error as e:
        logger.error(f"Error fetching machine status data: {e}")
        return 0, 0, 0, [], []

    finally:
        conn.close()


# ════════════════════════════════════════════════════════════════════════════
#  EMAIL BUILDER
# ════════════════════════════════════════════════════════════════════════════

def build_machine_status_email():
    """Build full HTML email with machine status report — iOS friendly."""

    total, total_online, total_offline, location_stats, disconnected = get_machine_status_data()
    now_str = datetime.now().strftime("%d %b %Y %H:%M")
    gen_str = datetime.now().strftime("%d-%m-%Y %H:%M:%S")

    slot_label = "Morning Report"

    # ── Location-wise breakdown ───────────────────────────────────────────
    location_blocks_html = ""

    if location_stats:
        for loc_idx, loc in enumerate(location_stats):
            location_blocks_html += f"""
            <tr style="background:#1a3c78;">
                <td colspan="2"
                    style="padding:8px 10px; border:1px solid #2c5f9e;
                           color:#fff; font-weight:bold; font-size:12px;">
                    📍 {loc['location_name']}
                    <span style="font-weight:normal; font-size:11px; color:#a8c4f0;">
                        &nbsp;—&nbsp; {loc['offline_count']} offline
                    </span>
                </td>
            </tr>"""

            for m_idx, m in enumerate(loc["machines"]):
                bg = "#fff5f5" if m_idx % 2 == 0 else "#ffffff"
                message_cell = (
                    f'<span style="color:#666; font-size:11px;">{m["message_string"]}</span>'
                    if m["message_string"]
                    else '<span style="color:#bbb; font-size:11px;">—</span>'
                )
                location_blocks_html += f"""
                <tr style="background:{bg};">
                    <td style="padding:8px 10px; border:1px solid #f0d0d0;
                               font-weight:bold; font-size:12px; width:50%;">
                        🔴 {m['machine_name']}
                        <div style="font-size:10px; color:#888; font-weight:normal;">
                            ID: {m['machine_id']} &nbsp;|&nbsp; {m['category_name']}
                        </div>
                        <div style="font-size:10px; color:#aaa; font-weight:normal;">
                            v{m['build_version']}
                        </div>
                    </td>
                    <td style="padding:8px 10px; border:1px solid #f0d0d0;
                               font-size:11px; width:50%; vertical-align:top;">
                        {message_cell}
                    </td>
                </tr>"""

        location_blocks_html += """
            <tr>
                <td colspan="2" style="padding:4px; background:#f0f4f8; border:none;"></td>
            </tr>"""

    else:
        location_blocks_html = """
            <tr>
                <td colspan="2"
                    style="padding:16px; text-align:center; color:#1e8a2e; font-weight:bold;">
                    ✅ All machines are online!
                </td>
            </tr>"""

    # ── Offline section ───────────────────────────────────────────────────
    if disconnected:
        offline_section = f"""
        <p style="color:#cc0000; font-weight:bold; font-size:13px;
                  margin-top:24px; font-family:Arial,sans-serif;">
            🔴 Location-wise Offline Breakdown — {total_offline} machine(s) offline
        </p>
        <table style="border-collapse:collapse; width:100%;
                      font-family:Arial,sans-serif; font-size:12px; table-layout:fixed;">
            <thead>
                <tr style="background:#cc0000; color:#fff;">
                    <th style="padding:8px 10px; border:1px solid #f0d0d0;
                               text-align:left; width:50%;">
                        Machine Name
                    </th>
                    <th style="padding:8px 10px; border:1px solid #f0d0d0;
                               text-align:left; width:50%;">
                        Last Message
                    </th>
                </tr>
            </thead>
            <tbody>
                {location_blocks_html}
            </tbody>
        </table>"""
    else:
        offline_section = """
        <div style="margin-top:20px; padding:14px 18px; background:#e6f4ea;
                    border-left:5px solid #1e8a2e; border-radius:4px;
                    font-family:Arial,sans-serif;">
            <strong style="color:#1e8a2e; font-size:14px;">
                ✅ All machines are currently Online!
            </strong>
        </div>"""

    # ── Full HTML body ────────────────────────────────────────────────────
    body = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
        <meta http-equiv="Content-Type" content="text/html; charset=UTF-8"/>
    </head>
    <body style="margin:0; padding:8px; background:#f0f4f8;
                 font-family:Arial,sans-serif; color:#333; -webkit-text-size-adjust:100%;">

        <table width="100%" cellpadding="0" cellspacing="0"
               style="max-width:600px; margin:0 auto;">
            <tr>
                <td>

                    <!-- Header -->
                    <div style="background:#1a3c78; padding:16px 18px;
                                border-radius:8px 8px 0 0;">
                        <div style="color:#fff; font-size:17px; font-weight:bold;">
                            📊 Machine Status Report
                        </div>
                        <div style="color:#a8c4f0; font-size:12px; margin-top:4px;">
                            {slot_label} &nbsp;·&nbsp; {now_str}
                        </div>
                    </div>

                    <!-- Content card -->
                    <div style="background:#fff; padding:16px 18px;
                                border-radius:0 0 8px 8px;">

                        <!-- Summary cards — stacked for mobile -->
                        <table width="100%" cellpadding="0" cellspacing="0"
                               style="margin-bottom:16px;">
                            <tr>
                                <td style="padding:4px;">
                                    <div style="background:#e8f0fe; padding:12px 8px;
                                                border-radius:8px; text-align:center;">
                                        <div style="font-size:11px; color:#555;">
                                            Total Machines
                                        </div>
                                        <div style="font-size:28px; font-weight:bold;
                                                    color:#1a3c78;">
                                            {total}
                                        </div>
                                    </div>
                                </td>
                                <td style="padding:4px;">
                                    <div style="background:#e6f4ea; padding:12px 8px;
                                                border-radius:8px; text-align:center;">
                                        <div style="font-size:11px; color:#555;">
                                            🟢 Online
                                        </div>
                                        <div style="font-size:28px; font-weight:bold;
                                                    color:#1e8a2e;">
                                            {total_online}
                                        </div>
                                    </div>
                                </td>
                                <td style="padding:4px;">
                                    <div style="background:#fce8e6; padding:12px 8px;
                                                border-radius:8px; text-align:center;">
                                        <div style="font-size:11px; color:#555;">
                                            🔴 Offline
                                        </div>
                                        <div style="font-size:28px; font-weight:bold;
                                                    color:#cc0000;">
                                            {total_offline}
                                        </div>
                                    </div>
                                </td>
                            </tr>
                        </table>

                        <!-- Offline breakdown -->
                        {offline_section}

                        <!-- Footer -->
                        <p style="margin-top:24px; color:#bbb; font-size:10px;
                                  border-top:1px solid #eee; padding-top:10px;">
                            Generated at {gen_str} &nbsp;|&nbsp; Consumer2 Machine Monitor
                            &nbsp;|&nbsp; Report Schedule: 10:00 AM Daily
                        </p>

                    </div>
                </td>
            </tr>
        </table>

    </body>
    </html>
    """
    return body


# ════════════════════════════════════════════════════════════════════════════
#  EMAIL SENDERS
# ════════════════════════════════════════════════════════════════════════════

def send_email(subject, body):
    """Send plain-text email to all receivers in EMAIL_RECEIVER_LIST."""
    msg = MIMEText(body)
    msg['Subject'] = subject
    msg['From']    = EMAIL_SENDER
    msg['To']      = ", ".join(EMAIL_RECEIVER_LIST)
    try:
        with smtplib.SMTP(SMTP_SERVER, int(SMTP_PORT)) as server:
            server.starttls()
            server.login(EMAIL_SENDER, EMAIL_PASSWORD)
            server.sendmail(EMAIL_SENDER, EMAIL_RECEIVER_LIST, msg.as_string())
        logger.info(f"Email sent: {subject}")
    except Exception as e:
        logger.error(f"Failed to send email: {e}")


def send_html_email(subject, html_body):
    """Send HTML email to all receivers in EMAIL_RECEIVER_LIST."""
    msg = MIMEMultipart("alternative")
    msg['Subject'] = subject
    msg['From']    = EMAIL_SENDER
    msg['To']      = ", ".join(EMAIL_RECEIVER_LIST)
    msg.attach(MIMEText(html_body, "html"))
    try:
        with smtplib.SMTP(SMTP_SERVER, int(SMTP_PORT)) as server:
            server.starttls()
            server.login(EMAIL_SENDER, EMAIL_PASSWORD)
            server.sendmail(EMAIL_SENDER, EMAIL_RECEIVER_LIST, msg.as_string())
        logger.info(f"HTML report email sent: {subject}")
    except Exception as e:
        logger.error(f"Failed to send HTML email: {e}")


# ════════════════════════════════════════════════════════════════════════════
#  SCHEDULED REPORT  (10:00 AM daily)
# ════════════════════════════════════════════════════════════════════════════

def schedule_machine_report():
    """
    Background thread — fires machine status email at 10:00 AM every day.
    Wrapped in try/except so a single failure never kills the thread.
    """
    logger.info(
        f"Machine report scheduler started. "
        f"Send times: {', '.join(REPORT_TIMES)}"
    )

    while True:
        try:
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

            now_label = datetime.now().strftime("%d %b %Y %H:%M")
            subject   = f"Machine Status Report — {now_label}"
            body      = build_machine_status_email()
            send_html_email(subject, body)
            logger.info(f"Machine status report sent at {now_label}")

        except Exception as e:
            logger.error(
                f"[schedule_machine_report] Error — will retry next cycle: {e}",
                exc_info=True
            )

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
        hb             = message.value
        producer_id    = hb['producer_id']
        producer_name  = hb['producer_name']
        location_id    = hb['location_id']
        last_heartbeat = datetime.fromisoformat(hb['timestamp'])

        logger.info(f"Received heartbeat: {hb}")

        update_producer_status(producer_id, producer_name, location_id, last_heartbeat, 1)


# ════════════════════════════════════════════════════════════════════════════
#  SIGNAL HANDLERS
# ════════════════════════════════════════════════════════════════════════════

def handle_interrupt(signum, frame):
    logger.info("Keyboard interrupt detected. Shutting down consumer2.")
    os._exit(0)


def handle_shutdown(signum, frame):
    logger.info("System is shutting down.")
    os._exit(0)


# ════════════════════════════════════════════════════════════════════════════
#  KAFKA CONSUMER — MESSAGE PROCESSING
# ════════════════════════════════════════════════════════════════════════════

def consume_messages():
    DB_RETRY_WAIT = 10   # seconds to wait before reconnecting after a DB failure

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
        # ── Establish (or re-establish) DB connection ─────────────────────
        conn   = None
        cursor = None
        try:
            conn   = get_db_connection()
            cursor = conn.cursor()
            logger.info("Database connection established.")
        except pyodbc.Error as e:
            logger.error(f"Failed to connect to database: {e}. Retrying in {DB_RETRY_WAIT}s...")
            time.sleep(DB_RETRY_WAIT)
            continue   # restart the outer while loop to retry connection

        # ── Message poll loop (runs until a DB error forces a reconnect) ──
        try:
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

                except pyodbc.Error as e:
                    # DB-level error: rollback and break out to reconnect
                    logger.error(f"DB error during batch processing: {e}. Reconnecting...")
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                    break   # exit inner while → outer while will reconnect

                except Exception as e:
                    # Non-DB processing error: rollback and keep going
                    logger.error(f"Processing error during batch: {e}")
                    try:
                        conn.rollback()
                    except Exception:
                        pass

        except Exception as e:
            logger.error(f"Unexpected error in message loop: {e}. Reconnecting in {DB_RETRY_WAIT}s...")
            time.sleep(DB_RETRY_WAIT)

        finally:
            # Always clean up the current connection before the outer loop retries
            if cursor:
                try:
                    cursor.close()
                except Exception:
                    pass
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass


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

    # Machine status report — 10:00 AM daily
    threading.Thread(
        target=schedule_machine_report,
        name="MachineReportScheduler",
        daemon=True
    ).start()

    logger.info("All threads started. Running consume_messages...")
    consume_messages()