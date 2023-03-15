# -*- coding:utf-8 -*-
# Author: lq
# data:  10:40 AM
# file: mtest.py
# 8.62/11.73
import os
import time
import pandas as pd
from concurrent.futures import ThreadPoolExecutor

import requests

pool = ThreadPoolExecutor(os.cpu_count())

session_ = requests.Session()


def spider(number):
    with session_.get(
            "https://publicdata.caida.org/datasets/topology/ark/ipv4/probe-data/team-1/2022/cycle-20220302/") as se:
        print(f"{number}:", se.status_code)


t = time.perf_counter()
for i in range(os.cpu_count() + 5):
    pool.map(spider, (i,))
print(time.perf_counter() - t)
# 0.005735908052884042
# 10.797772324993275
