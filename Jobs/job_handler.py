# -*- coding: UTF-8 -*-

import gevent
import logging
import psycopg2
import os

from datetime import datetime
from util.helper import select_rows, update_row, exception_callback
from bestprice.bestprice_scrape import BestPriceScrape

logger = logging.getLogger("JobHandler_{}".format(os.getpid()))


class JobHandler:
    def __init__(self, conn, result_handler):
        self.conn = conn
        self.result_handler = result_handler
        self.reload = True
        self.process_id = os.getpid()

    def job_updater(self):
        logger.info("job_updater.......")
        while True:
            if self.reload:
                try:
                    query = "SELECT app_job.id as job_id, app_job.job_params_id, post_code, delivery_date, collection_date " \
                            "FROM app_job INNER JOIN app_jobparams ON app_jobparams.id = app_job.job_params_id"\
                            " INNER JOIN app_processinfo on app_job.process_id=app_processinfo.id"\
                            " WHERE app_job.status in ('NEW', 'RESTART') AND app_processinfo.process_id={}".format(self.process_id)
                    cursor = select_rows(self.conn, query)
                    for row in cursor.fetchall():
                        row = dict(row)
                        gevent.spawn(BestPriceScrape(row=row, result_handler=self.result_handler).start_scrape)
                        update_row(self.conn, "UPDATE app_job set status='{}', started_at='{}' WHERE id={}"
                                   .format("RUNNING", str(datetime.now()), row["job_id"]))
                except psycopg2.errors.InFailedSqlTransaction as ex:
                    logger.error(str(ex))
                except Exception as ex:
                    logger.error(ex.__class__)
                    logger.error(str(ex))
                self.reload = False
            gevent.sleep(10)
