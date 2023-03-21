# -*- coding:utf-8 -*-
# Author: lq
# data:  10:20 AM
# file: hdfs_operation.py
from hdfs import InsecureClient


def init_hdfs_client(url, user):
    if url is None:
        url = 'http://10.42.1.25:9870;http://10.42.1.26:9870'
    if user is None:
        user = "flink"
    client = InsecureClient(url=url, user=user)

    return client
