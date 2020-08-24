from gevent import monkey
monkey.patch_all()

import logging
import requests
import random
import psycopg2

from gevent import sleep
from psycopg2.extras import RealDictCursor
from bs4 import BeautifulSoup
from dateutil import parser

logger = logging.getLogger("Helper")


def select_rows(connection, query):
    logger.info(query)
    cursor = connection.cursor(cursor_factory=RealDictCursor)
    cursor.execute(query)
    return cursor


def update_row(connection, query):
    try:
        logger.info(query)
        cur = connection.cursor()
        cur.execute(query)
        connection.commit()
    except psycopg2.DatabaseError as e:
        if connection:
            connection.rollback()


def build_url(link, key, value, first=False):
    return "{}?{}={}".format(link, key, value) if first else "{}&{}={}".format(link, key, value)


def request_data(url):
    try:
        res = requests.get(url=url)
        return res
    except Exception as ex:
        logger.error("error occurred;requested_url={};error={}".format(url, str(ex)))
        return None


def bs_parse(html):
    return BeautifulSoup(html, 'lxml')


def get_date_object(date_obj, date_time_format="%d-%m-%Y"):
    if isinstance(date_obj, str):
        date_obj = parser.parse(date_obj)
    return date_obj.strftime(date_time_format)


def exception_callback(greenlet):
    logging.warning("Exception happened in %r", greenlet)


def random_sleep(a=5, b=10):
    return sleep(random.randint(a, b))


def get_safely(rows, index, code):
    try:
        return eval(code)
    except Exception as ex:
        return ""
