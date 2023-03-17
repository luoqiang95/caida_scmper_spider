# -*- coding:utf-8 -*-
# Author: lq
# data:  3:48 PM
# file: db_operation.py
import pymysql
import sqlalchemy


def create_engine():
    USER = "root"
    PASSWORD = "mininet"
    HOST = "192.168.60.137"
    DATABASE = "ty"
    PORT = 3306
    engine = sqlalchemy.create_engine(f"mysql+pymysql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}", echo=True)
    conn = pymysql.Connection(user=USER, host=HOST, database=DATABASE, port=PORT, password=PASSWORD)
    return engine, conn
