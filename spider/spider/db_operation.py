# -*- coding:utf-8 -*-
# Author: lq
# data:  3:48 PM
# file: db_operation.py
import pymysql
import sqlalchemy


def create_engine():
    USER = "lq"
    PASSWORD = "123456"
    HOST = "172.16.143.30"
    DATABASE = "topo_analysis"
    PORT = 22624
    engine = sqlalchemy.create_engine(f"mysql+pymysql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}", echo=True)
    conn = pymysql.Connection(user=USER, host=HOST, database=DATABASE, port=PORT, password=PASSWORD)
    return engine, conn
