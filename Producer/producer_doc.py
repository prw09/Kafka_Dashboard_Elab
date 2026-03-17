import json
import pyodbc
import logging
import os
import sys
import time
import signal
import shutil
import threading
from datetime import datetime, timedelta
from kafka import KafkaProducer
from logging.handlers import TimedRotatingFileHandler


try:
    import win32serviceutil
    import win32service
    import win32event
    import servicemanager
except ImportError:
    win32serviceutil = None




