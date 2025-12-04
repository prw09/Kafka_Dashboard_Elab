import os
import shutil
import time
import threading
from datetime import datetime
from kafka import KafkaConsumer
import json

LOG_DIR = r"E:\office\KafkaProject\Producer\logs"     # Producer log folder
BATKAFKA_DIR = r"E:\office\KafkaProject\Consumer\batkafka"  # Daily folder base
CLEAR_INTERVAL_SEC = 60  # Run every 1 minute


def move_logs_daily():
    """Moves log files into today's date folder inside batkafka without deleting old logs."""
    if not os.path.exists(LOG_DIR):
        print(f"⚠️ Log folder does not exist: {LOG_DIR}")
        return

    # Create daily folder (one per day)
    today_folder_name = datetime.now().strftime("%Y-%m-%d")
    today_folder_path = os.path.join(BATKAFKA_DIR, today_folder_name)
    os.makedirs(today_folder_path, exist_ok=True)

    moved = False
    for filename in os.listdir(LOG_DIR):
        src_path = os.path.join(LOG_DIR, filename)

        if os.path.isfile(src_path) and filename.endswith(".log"):
            moved = True
            try:
                # Create UNIQUE filename to avoid overwrite
                timestamp = datetime.now().strftime("%H-%M-%S")
                base, ext = os.path.splitext(filename)
                new_filename = f"{base}_{timestamp}{ext}"

                dest_path = os.path.join(today_folder_path, new_filename)

                # Move log file safely (NO DELETE)
                shutil.move(src_path, dest_path)

                print(f"📦 Moved: {filename} → {new_filename} in {today_folder_path}")

            except Exception as e:
                print(f"❌ Error moving file {filename}: {e}")

    if not moved:
        print("ℹ️ No log files found to move.")


def scheduler():
    """Runs the log-moving job every minute."""
    while True:
        time.sleep(CLEAR_INTERVAL_SEC)
        move_logs_daily()


def consume_messages():
    """Your REAL Kafka consumer code."""
    try:
        consumer = KafkaConsumer(
            'your_topic_name',
            bootstrap_servers='localhost:9092',
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='consumer-group',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )

        print("🚀 Kafka Consumer Started...")

        for message in consumer:
            print(f"📩 Received: {message.value}")

    except Exception as e:
        print(f"❌ Kafka Error: {e}")


if __name__ == "__main__":
    print("🚀 Daily Log Mover + Kafka Consumer Started...")

    # Thread 1 – Daily Log Moving
    threading.Thread(target=scheduler, daemon=True).start()

    # Thread 2 – Kafka Consumer
    threading.Thread(target=consume_messages, daemon=True).start()

    # Keep main alive
    while True:
        time.sleep(1)
