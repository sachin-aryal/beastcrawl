from gevent import monkey
monkey.patch_all()

import logging.handlers
import os
import sys
import gevent
import psycopg2
import configparser
import signal


from result_handler import ResultHandler
from job_handler import JobHandler


logger = logging.getLogger("Task Runner_{}".format(os.getpid()))
CURRENT_PATH = os.path.dirname(os.path.abspath(__file__))
DB_CONF_PATH = os.path.join(CURRENT_PATH, "postgres.cnf")

job_generator = None


def poll_db(sighhup, frame):
    logger.info("reload signal received.......")
    job_generator.reload = True


signal.signal(signal.SIGHUP, poll_db)


def parse_config():
    if len(sys.argv) > 1:
        env = sys.argv[1]
    else:
        env = "DEVELOPMENT"
    config = configparser.ConfigParser()
    config.read(DB_CONF_PATH)
    return config[env]


def connect_db(config):
    connection = psycopg2.connect(user=config["USER"],
                                  password=config["PASSWORD"],
                                  host=config["HOST"],
                                  port=config["PORT"],
                                  database=config["DATABASE"])
    logger.info(connection.dsn)
    return connection


def main():
    config = parse_config()
    conn = connect_db(config)
    event_handler = ResultHandler(conn)
    global job_generator
    job_generator = JobHandler(conn, event_handler)

    try:
        eventlet = gevent.spawn(event_handler.result_queue_handler)
        joblet = gevent.spawn(job_generator.job_updater)
        gevent.joinall([joblet, eventlet])
    except gevent.GreenletExit as err:
        logger.warning(err)
    except Exception as err:
        logger.warning(err)


if __name__ == "__main__":
    fh = logging.handlers.RotatingFileHandler(os.path.join(CURRENT_PATH, "log", "background_job.log"),
                                              maxBytes=1000000, backupCount=10)
    fh.setLevel(logging.DEBUG)

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.DEBUG)

    fh.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    ch.setFormatter(logging.Formatter('%(name)s - %(levelname)s - %(message)s'))

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    root.addHandler(fh)
    root.addHandler(ch)

    main()
