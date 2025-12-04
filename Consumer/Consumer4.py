import os
import shutil
import time
import threading
from datetime import datetime, timedelta

# FOLDERS
LOG_DIR = r"E:\office\KafkaProject\Producer\logs"
BATKAFKA_DIR = r"E:\office\KafkaProject\Consumer\batkafka"
CLEAR_INTERVAL_SEC = 10   # check every 10 sec

# 2 MB for testing
MAX_SIZE_BYTES = 2 * 1024 * 1024

# Delay before consuming
DELAY_MINUTES = 5


def move_logs_daily():
    """Rotate ONLY producer.log when >2MB and 5 minutes old."""

    log_file = os.path.join(LOG_DIR, "producer.log")

    if not os.path.exists(log_file):
        print("⚠️ producer.log missing — waiting...")
        return

    size = os.path.getsize(log_file)

    if size < MAX_SIZE_BYTES:
        print("ℹ️ producer.log < 2MB, nothing to move.")
        return

    # --------------------------------------------
    #  FIXED: Use MODIFICATION time (not create time)
    # --------------------------------------------
    modified_ts = os.path.getmtime(log_file)
    modified_time = datetime.fromtimestamp(modified_ts)
    age_minutes = (datetime.now() - modified_time).total_seconds() / 60

    if age_minutes < DELAY_MINUTES:
        remaining = DELAY_MINUTES - age_minutes
        print(f"⏳ producer.log >2MB but waiting {remaining:.1f} more minutes...")
        return

    # Create today's folder
    today_folder = datetime.now().strftime("%Y-%m-%d")
    today_path = os.path.join(BATKAFKA_DIR, today_folder)
    os.makedirs(today_path, exist_ok=True)

    # Unique rotated filename
    timestamp = datetime.now().strftime("%H-%M-%S")
    rotated_name = f"producer_{timestamp}.log"
    dest_path = os.path.join(today_path, rotated_name)

    try:
        shutil.move(log_file, dest_path)
        print(f"📦 Rotated: producer.log → {dest_path}")

        open(log_file, 'w').close()
        print("🆕 Created new producer.log (empty)")

    except Exception as e:
        print(f"❌ Error rotating log: {e}")


def scheduler():
    while True:
        time.sleep(CLEAR_INTERVAL_SEC)
        print("\n⏱️ Checking producer.log...")
        move_logs_daily()


def consume_messages():
    while True:
        time.sleep(3)
        print("📨 Consumer running...")


if __name__ == "__main__":
    print("🚀 Kafka Consumer + Log Rotator Started...")

    threading.Thread(target=scheduler, daemon=True).start()
    threading.Thread(target=consume_messages, daemon=True).start()

    while True:
        time.sleep(1)
