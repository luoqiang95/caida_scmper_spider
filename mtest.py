# -*- coding:utf-8 -*-
# Author: lq
# data:  10:40 AM
# file: mtest.py
import os

print(os.path.dirname(__file__))
base = "https://publicdata.caida.org/datasets/topology/ark/ipv4/probe-data/team-1/"
a =    "https://publicdata.caida.org/datasets/topology/ark/ipv4/probe-data/team-1/2022/"
print(a.replace(base, ''))
print(int("0x1c55fd0",16))