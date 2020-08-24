import logging
import json
import os


from datetime import datetime
from util.helper import update_row
from gevent.queue import Queue

logger = logging.getLogger("ResultHandler_{}".format(os.getpid()))


class ResultHandler:
    def __init__(self, conn):
        self.data_queue = Queue()
        self.conn = conn
        self.process_id = os.getpid()

    def result_queue_handler(self):
        logger.info("result_queue_handler started.......")
        while True:
            row = self.data_queue.get()
            update_row(self.conn, "UPDATE app_processinfo SET job_count = job_count - 1 WHERE process_id = "
                                  "{}".format(self.process_id))
            try:
                job_id = row["job_id"]
                status = "COMPLETED"
                if row["data"].get("error"):
                    status = "ERROR"
                data = json.dumps(row["data"])
                update_row(self.conn, query="UPDATE app_job SET result = '{}', completed_at='{}', status='{}' "
                                            "WHERE id={}".format(data, str(datetime.now()), status, job_id))
            except Exception as ex:
                logger.error(f"error={str(ex)}")

    def add_event(self, row):
        self.data_queue.put(row)
