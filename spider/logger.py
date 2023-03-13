# -*- coding:utf-8 -*-
# Author: lq
# data:  3:09 PM
# file: logger.py
import os
import datetime
import logging
from logging.handlers import TimedRotatingFileHandler

log_path = os.path.join(os.path.dirname(__file__), "log")
filename = f"{datetime.date}.log"
log_format = "%(asctime) %(name)-12s %(levelname)-8s %(message)s"
logging.basicConfig(filename=filename, format=log_format, datefmt="%m-%d %H:%M", level=logging.DEBUG)
console = TimedRotatingFileHandler(filename, when='D', interval=1)
console.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)
logger = logging.getLogger('')
