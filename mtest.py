# -*- coding:utf-8 -*-
# Author: lq
# data:  10:40 AM
# file: mtest.py
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
for i in range(os.cpu_count()):
    pool.map(spider, (i,))
print(time.perf_counter() - t)
# 0.005735908052884042
# 10.797772324993275
size = 22660710
capital = size // os.cpu_count()
b_l = []
for i in range(os.cpu_count()):
    if i == os.cpu_count() - 1:
        b_l.append(f"bytes{i * capital}-{size}")
    else:
        b_l.append(f"bytes{i * capital}-{(i + 1) * capital - 1}")
# print(b_l)
b_l = ['bytes5665177-11330353', 'bytes0-5665176', 'bytes16995531-22660710', 'bytes11330354-16995530']
a = sorted(b_l, key=lambda x: int(x.split("-")[-1]))
print(a[-1])
