# -*- coding:utf-8 -*-
# Author: lq
# data:  11:21 AM
# file: main.py
from downloader import main

if __name__ == '__main__':
    base_url = "https://publicdata.caida.org/datasets/topology/ark/ipv4/probe-data/team-1/2022/cycle-20220302/aep-ar.team-probing.c009902.20220303.warts.gz"
    main(base_url=base_url)
