#!/usr/bin/env python3

import requests
import json
import datetime
import os

import mysql.connector as mysql

from secrets.secrets import SQL_HOST, SQL_PASSWORD, SQL_USERNAME

SUMMARY_INDEX = "chtc-schedd-*"
ENDPOINT = "http://localhost:9200"


def get_mysql_connection() -> mysql.Connect:
    return mysql.connect(
        host=SQL_HOST,
        user=SQL_USERNAME,
        password=SQL_PASSWORD
    )


def projects_running_on_osg() -> list:  # TODO
    """Grabs CHTC projects that have run on the OSPool from ES"""
    pass


def projects_from_db() -> list:
    """Grabs projects from CHTC user db"""

    cnx = get_mysql_connection()

    cur = cnx.cursor()

    cur.execut
