import os
import shutil

src = r"E:\office\KafkaProject\Producer\logs\testfile.log"
dst = r"E:\office\KafkaProject\Consumer\batkafka\test_output.log"

# create test file
open(src, "w").write("hello")

try:
    shutil.move(src, dst)
    print("MOVED SUCCESSFULLY!")
except Exception as e:
    print("MOVE FAILED:", e)
