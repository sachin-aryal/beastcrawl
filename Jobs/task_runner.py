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
from util.helper import update_row

PID = os.getpid()
logger = logging.getLogger("Task Runner_{}".format(PID))
CURRENT_PATH = os.path.dirname(os.path.abspath(__file__))
DB_CONF_PATH = os.path.join(CURRENT_PATH, "postgres.cnf")

job_generator = connection = environment = None


def poll_db(sighhup, frame):
    logger.info("reload signal received.......")
    job_generator.reload = True


def exit_gracefully(signum, frame):
    try:
        update_row(connection,
                   "UPDATE app_processinfo SET status = 'KILLED' WHERE process_id = {}".format(PID))
    except Exception as ex:
        pass
    sys.exit(0)


signal.signal(signal.SIGHUP, poll_db)
signal.signal(signal.SIGINT, exit_gracefully)
signal.signal(signal.SIGTERM, exit_gracefully)


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
    global job_generator, connection
    config = parse_config()
    connection = connect_db(config)
    event_handler = ResultHandler(connection)
    job_generator = JobHandler(connection, event_handler)

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
