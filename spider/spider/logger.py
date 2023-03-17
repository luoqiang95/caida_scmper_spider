# -*- coding:utf-8 -*-
# Author: lq
# data:  3:09 PM
# file: logger.py
import os
import datetime
import logging
from logging.handlers import TimedRotatingFileHandler


def init_logger(log_path=None):
    log_path = log_path if log_path else os.path.join(os.path.dirname(__file__), "log")
    if not os.path.exists(log_path):
        os.mkdir(log_path)
    filename = os.path.join(log_path, "spider.log")
    log_format = "%(asctime)s %(name)s %(levelname)s %(message)s"
    logging.basicConfig(filename=filename, format=log_format, datefmt="%m-%d %H:%M", level=logging.DEBUG)
    file_handler = TimedRotatingFileHandler(filename, when='D', interval=1)
    file_handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s %(name)s: %(levelname)s %(message)s')
    file_handler.setFormatter(formatter)
    logging.getLogger('').addHandler(file_handler)
    console_handler = logging.StreamHandler()
    # console_handler.setFormatter(formatter)
    logging.getLogger('').addHandler(console_handler)
    logger = logging.getLogger('')
    return logger
