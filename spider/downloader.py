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
from queue import Queue
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
    base_path = os.path.dirname(__file__) + os.sep + "files"

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
        if url.endswith("warts.gz"):
            self.get_download_headers(url)
            if self.get_file_info(url):
                filename = url.split("/")[-1]
                v = self.file_mapper.get(filename)
                _range = f"bytes=0-{v['size']}"
                self.download_warts_file(url, _range, 0)
        else:
            with self.session.get(url, headers=self.base_headers, timeout=5) as se:
                html = etree.HTML(se.text, base_url=url)
            if html is not None:
                self.parse_html(html)

    def parse_html(self, html):
        a_list = html.xpath("//a/@href")
        self.create_dirs_or_file(html.base, a_list[5:])

    def create_dirs_or_file(self, url, a_list):
        path_suffix = url.replace(self._base_url, '').split("/")[:-1]
        if path_suffix:
            current_path_rel = f"{os.sep}".join(path_suffix)
            path = os.path.join(self.base_path, current_path_rel)
            if not os.path.exists(path):
                os.makedirs(path)
        else:
            path = self.base_path
        path_dic = self.pwd(path_suffix)
        for a_u in a_list:
            if a_u.endswith("warts.gz"):
                new_file_path = os.path.join(path, a_u)
                if not os.path.exists(new_file_path):
                    os.mknod(new_file_path)
                    print(f"create new file {new_file_path}")
                a = path_suffix.copy()
                a.append(a_u)
                file_path = self.base_url + "/".join(a)
                path_dic[a_u] = file_path
                url = self._base_url + "/".join(path_suffix) + "/"
                self.input_file_info(a_u, path=file_path, url=url)
            else:
                new_dir = os.path.join(path, a_u)
                if not os.path.exists(new_dir):
                    os.mkdir(new_dir)
                    print(f"create new dir {new_dir}")
                path_dic[a_u] = {}

    def input_file_info(self, filename, **kwargs):
        if not self.file_mapper.get(filename):
            self.file_mapper[filename] = kwargs
        else:
            self.file_mapper[filename].update(kwargs)

    def pwd(self, path_suffix):
        """
        locating the current file should in where path mapper
        """
        path_tree = self.path_mapper
        for fix in path_suffix:
            dic = path_tree.get(fix)
            if dic is None:
                new_dic = {}
                path_tree[fix] = new_dic
                path_tree = new_dic
            else:
                path_tree = dic
        return path_tree

    def get_download_headers(self, url, _range=None):
        filename = url.split("/")[-1]
        referer = url.replace(filename, '')
        headers = {
            'If-Range': '"18a106b-5d93f69e23de3"',
            'Range': _range if _range else 'bytes=1-1',
            'Referer': referer,
            'Sec-Fetch-Site': 'same-origin',
        }
        self.down_headers.update(headers)

    def download_warts_file(self, url, _range=None, number=None):
        filename = url.split("/")[-1]
        if _range is not None and number is not None:
            with self.session.get(url, headers=self.down_headers, timeout=5) as se:
                if se.status_code == 206:
                    self.create_file_object(number, se, filename)

    def get_file_info(self, url):
        filename = url.split("/")[-1]
        if not url.endswith(".warts.gz"):
            print("file link error!!!!")
            return
        url_split = url.split("/")
        date = url_split[-2].split("-")[-1]
        year = url_split[-3]
        with self.session.get(url, headers=self.down_headers, timeout=5) as se:
            if 200 <= se.status_code < 300:
                size, modified = self.get_file_size(se)
                self.df["filename"] = filename
                self.df[self.df["filename"] == filename]["size"] = size
                self.df[self.df["filename"] == filename]["modified"] = modified
                self.df[self.df["filename"] == filename]["date"] = date
                self.df[self.df["filename"] == filename]["year"] = year
                self.df[self.df["filename"] == filename]["url"] = url
                if se.headers.get("Content-Range") is not None:
                    self.input_file_info(filename, size=size, modified=modified)
                    return True
                path_suffix = url.replace(self._base_url, '').split("/")
                path = os.path.join(self.base_path, f'{os.sep}'.join(path_suffix))
                self.df[self.df["filename"] == filename]["path"] = path
                print(f"save file in path {path}")
                if os.path.exists(path):
                    with open(path, "ab") as fd:
                        fd.write(se.content)
                else:
                    with open(path, "wb") as fd:
                        fd.write(se.content)
        return False

    def create_file_object(self, number, session, filename):
        """
        create file object
        """
        file = BytesIO()
        for content in session.iter_content(1024 * 30):
            print("downloading....")
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
        e_tag = headers.get("ETag").replace('"', '')
        size = int("0x" + e_tag.split("-")[0], 16)
        modified = headers.get("Last-Modified")
        return size, modified

    def main(self):
        """
        spider scamper
        """
        self.get_html()
        for k, v in self.file_mapper.items():
            _range = f"bytes=0-{v['size']}"
            self.download_warts_file(v["url"], _range, 0)
        print(self.file_mapper)


# s = ScamperSpider(base_url="https://publicdata.caida.org/datasets/topology/ark/ipv4/probe-data/team-1/2019/")
s = ScamperSpider(
    base_url="https://publicdata.caida.org/datasets/topology/ark/ipv4/probe-data/team-1/2022/cycle-20220302/abz2-uk.team-probing.c009902.20220302.warts.gz")
s.main()
