import subprocess
import sys
import os

import psutil
from django.conf import settings


def start_background_job():
    proc = subprocess.Popen([sys.executable, os.path.join(settings.BASE_DIR, "Jobs", "task_runner.py"),
                             settings.CURRENT_ENVIRONMENT], close_fds=True)
    return proc.pid


def send_sighup_signal(process_id):
    try:
        os.system("kill -1 {}".format(process_id))
    except Exception as ex:
        print("error occurred while sending SIGHUP signal")


def kill_process(process_id):
    try:
        os.system("kill -9 {}".format(process_id))
    except Exception as ex:
        print("error occurred while killing process.")


def get_process_status(process_id):
    if process_id == 0:
        return False
    return psutil.pid_exists(process_id)
