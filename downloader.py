# -*- coding:utf-8 -*-
# Author: lq
# data:  下午4:34
# file: downloader.py
import os
import pymysql
import requests
import pandas as pd
from io import BytesIO
from lxml import etree
from threading import Thread
from concurrent.futures import ThreadPoolExecutor


class ScamperSpider:
    _base_url = "https://publicdata.caida.org/datasets/topology/ark/ipv4/probe-data/team-1/"
    thread_num = 5
    table_columns = ["filename", "year", "date", "down_time", "size", "path", "now_size", "over", "url", "modified"]
    base_headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Connection': 'keep-alive',
        'Host': 'publicdata.caida.org',
        'sec-ch-ua': '"Chromium";v="110", "Not A(Brand";v="24", "Google Chrome";v="110"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Linux"',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-User': '?1',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36',
    }
    base_path = os.path.dirname(__file__)

    def __init__(self, base_url=None, start_time=None, end_time=None):
        self.base_url = self._base_url if not base_url else base_url
        self.start_time = start_time
        self.end_time = end_time
        self.down_headers = self.base_headers.copy()
        self.session = requests.session()
        self.file_mapper = dict()  # {"filename":{"file":[],"size":"","modified":"",""}}
        self.df = pd.DataFrame(
            columns=self.table_columns)
        self.pool = ThreadPoolExecutor(max_workers=self.thread_num)
        self.path_mapper = {}

    def get_html(self, _url=None, header=None):
        """
        get html source code, use etree parse label
        """
        url = _url if _url else self.base_url
        with self.session.get(url, headers=self.base_headers, timeout=5) as se:
            html = etree.HTML(se.text)
        if html is not None:
            a_list = html.xpath("//a/@href")
            path_suffix = url.replace(self._base_url, '').split("/")[:-1]
            if path_suffix:
                path = os.path.join(self.base_path, )
            else:
                path = self.base_path
            for a_u in a_list[5:]:
                new_dir = os.path.join(path, a_u)
                if not os.path.exists(new_dir):
                    os.makedirs(new_dir)
                path_dic = self.path_mapper
                for fix in path_suffix:
                    dic = path_dic.get(fix)
                    if dic is None:
                        path_dic[fix] = {}
                    else:
                        path_dic = dic
                path_dic[path_suffix[-1]] = {}
            print(self.path_mapper)

    def get_file_info(self, url):
        url_split = url.split("/")
        filename = url_split[-1]
        date = url_split[-2].split("-")[-1]
        year = url_split[-3]
        with self.session.get(url, headers=self.down_headers, timeout=5) as se:
            if se.status_code == 206:
                size, modified = self.get_file_size(se)
                self.file_mapper[filename]["size"] = size
                self.file_mapper[filename]["modified"] = modified
                self.df["filename"] = filename
                self.df[self.df["filename"] == filename]["size"] = size
                self.df[self.df["filename"] == filename]["modified"] = modified
                self.df[self.df["filename"] == filename]["date"] = date
                self.df[self.df["filename"] == filename]["year"] = year

    def download_warts_file(self, url, referer, _range=None, number=None):
        headers = {
            'If-Range': '"18a106b-5d93f69e23de3"',
            'Range': _range if _range else 'bytes=1-1',
            'Referer': referer,
            'Sec-Fetch-Site': 'same-origin',
        }

        self.down_headers.update(headers)
        if _range is not None and number is not None:
            with self.session.get(url, headers=self.down_headers, timeout=5) as se:
                if se.status_code == 206:
                    self.create_file_object(number, se, filename)

    def create_file_object(self, number, session, filename):
        """
        create file object
        """
        file = BytesIO()
        for content in session.iter_content(1024 * 30):
            file.write(content)
        if self.file_mapper.get(filename):
            if not self.file_mapper[filename].get("file"):
                self.file_mapper[filename]["file"] = [(number, file)]
            else:
                self.file_mapper[filename]["file"].append((number, file))

    def save_warts_file(self):
        """
        save warts file
        """
        for filename, file_info in self.file_mapper.items():
            file_path = file_info.get("path")
            if not os.path.exists(file_path):
                fd = open(file_path, "wb")
            else:
                fd = open(file_path, "ab")
            files = sorted(file_info.get("file"), key=lambda x: x[0])
            now_size = 0
            for file in files:
                fd.write(file.read())
                now_size += file.__sizeof__()
            fd.close()
            self.file_mapper[filename]["now_size"] = now_size

    def get_file_size(self, response):
        """
        get file size
        """
        headers = response.headers
        size = headers.get("Content-Range")
        modified = headers.get("Last-Modified")
        return size, modified


s = ScamperSpider(base_url="https://publicdata.caida.org/datasets/topology/ark/ipv4/probe-data/team-1/2019/")
s.get_html()
