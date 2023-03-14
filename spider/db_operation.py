# -*- coding:utf-8 -*-
# Author: lq
# data:  3:48 PM
# file: db_operation.py
import pymysql

mysql_config = {
    "user": "root",
    "password": "mininet",
    "host": "192.168.60.137",
    "database": "ty",
    "port": 3306
}
conn = pymysql.Connection(**mysql_config)
