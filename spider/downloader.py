# -*- coding:utf-8 -*-
# Author: lq
# data:  下午4:34
# file: downloader.py
import os
import requests
import pandas as pd
from io import BytesIO
from lxml import etree
from queue import Queue
from logger import logger
from threading import Thread
from datetime import datetime
from db_operation import conn
from concurrent.futures import ThreadPoolExecutor


class ScamperSpider:
    _base_url = "https://publicdata.caida.org/datasets/topology/ark/ipv4/probe-data/team-1/"
    cpu_count = os.cpu_count()
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
    __base_path = os.path.dirname(__file__) + os.sep + "files"

    def __init__(self, base_path=None, base_url=None, start_time=None, end_time=None):
        self.base_url = self._base_url if not base_url else base_url
        self.down_headers = self.base_headers.copy()
        self.session = requests.session()
        self.file_mapper = dict()  # {"filename":[file_obj,file_obj]}
        self.df = pd.DataFrame(
            columns=self.table_columns)
        self.date_range = pd.date_range(start_time, end_time) if start_time and end_time else None
        self.base_path = base_path if base_path else self.__base_path
        self.pool = ThreadPoolExecutor(max_workers=self.cpu_count)
        self.queue = Queue()
        self.queue.put((base_url, None))

    def get_html(self, _url=None, header=None):
        """
        get html source code, use etree parse label
        """
        url = _url[0]
        if url.endswith("warts.gz"):
            print(url)
            self.get_download_headers(url, _url[1])
            self.get_file_info(url)
        else:
            try:
                with self.session.get(url, headers=self.base_headers, timeout=5) as se:
                    html = etree.HTML(se.text, base_url=url)
                if html is not None:
                    self.parse_html(html)
            except Exception as e:
                logger.error(msg=f"error! {_url}:")
                logger.error(msg=f"    {str(e)}")

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
        for a_u in a_list:
            new_dir = os.path.join(path, a_u)

            if not os.path.exists(new_dir):
                if a_u.endswith("warts.gz"):
                    os.mknod(new_dir)
                    logger.info(msg=f"create new file {new_dir}")
                else:
                    os.mkdir(new_dir)
                    logger.info(msg=f"create new dir {new_dir}")
            new_url = url + a_u
            self.queue.put((new_url, None))

    def input_file_info(self, filename, **kwargs):
        if not self.file_mapper.get(filename):
            self.file_mapper[filename] = kwargs
        else:
            self.file_mapper[filename].update(kwargs)

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

    def download_warts_file(self, url, _range=None):
        filename = url.split("/")[-1]
        if _range is not None:
            with self.session.get(url, headers=self.down_headers, timeout=5) as se:
                if se.status_code == 206:
                    self.create_file_object(_range, se, filename)

    def get_file_info(self, url):
        filename = url.split("/")[-1]
        if not url.endswith(".warts.gz"):
            print("file link error!!!!")
            return
        url_split = url.split("/")
        date = url_split[-2].split("-")[-1]
        if self.date_range and date not in self.date_range:
            return
        year = url_split[-3]
        file_info_df = pd.DataFrame({
            "filename": filename,
            "date": date,
            "year": year,
            "url": url,
            "down_time": datetime.now(),
            # "now_size": 0,
            # "over": 0
        }, index=[0])
        try:
            with self.session.get(url, headers=self.down_headers, timeout=5) as se:
                if 200 <= se.status_code < 300:
                    size, modified = self.get_file_size(se)
                    logger.debug(msg=file_info_df)
                    if self.df[self.df.filename == filename].empty:
                        file_info_df["size"] = size
                        self.df = pd.concat([self.df, file_info_df], ignore_index=True)
                    else:
                        self.df.loc[self.df.filename == filename, "size"] = size
                        self.df.loc[self.df.filename == filename, "modified"] = modified
                        self.df.loc[self.df.filename == filename, "date"] = date
                        self.df.loc[self.df.filename == filename, "year"] = year
                        self.df.loc[self.df.filename == filename, "url"] = url
                        self.df.loc[self.df.filename == filename, "down_time"] = datetime.now()
                    if se.headers.get("Content-Range") is not None:
                        # self.input_file_info(filename, size=size, modified=modified, url=url)
                        capital = size // self.cpu_count
                        for i in range(self.cpu_count):
                            if i == self.cpu_count - 1:
                                _range = f"bytes={i * capital}-{(i + 1) * capital}"
                            else:
                                _range = f"bytes={capital * self.cpu_count - 1}-{size - capital * self.cpu_count}"
                            self.queue.put((url, _range))
                        return
                    path_suffix = url.replace(self._base_url, '').split("/")
                    path = os.path.join(self.base_path, f'{os.sep}'.join(path_suffix))
                    self.df.loc[self.df.filename == filename, "path"] = path
                    logger.info(msg=f"save file in path {path}")
                    if os.path.exists(path):
                        with open(path, "ab") as fd:
                            fd.write(se.content)
                    else:
                        with open(path, "wb") as fd:
                            fd.write(se.content)
                    self.df.loc[self.df.filename == filename, "now_size"] = size
                    self.df.loc[self.df.filename == filename, "over"] = 1
        except Exception as e:
            self.df = pd.concat([self.df, file_info_df], ignore_index=True)
            logger.error(msg=f"download {filename} error!!")
            logger.error(msg=f"{filename} url: {url}")
            logger.error(msg=f"{str(e)}")

    def create_file_object(self, number, session, filename):
        """
        create file object
        """
        file = BytesIO()
        # for content in session.iter_content(1024 * 30):
        #     print("downloading....")
        file.write(session.content)
        logger.info(msg=f"{filename} size: {file.__sizeof__()}")
        if self.file_mapper.get(filename):
            if not self.file_mapper.get(filename):
                self.file_mapper[filename] = [(number, file)]
            else:
                self.file_mapper[filename].append((number, file))

    def spider_all_dirs(self):
        """

        """
        while True:
            if not self.queue.empty():
                url = self.queue.get()
                self.get_html(url)

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
        for i in range(self.cpu_count):
            self.pool.map(self.spider_all_dirs)
        # self.spider_all_dirs()
        print(self.file_mapper)
        print(self.df)
        if not self.df.empty:
            self.df.to_sql("caida_file", conn, index=False, if_exists="append")


base_url = "https://publicdata.caida.org/datasets/topology/ark/ipv4/probe-data/team-1/2022/cycle-20220302/"
s = ScamperSpider(base_url=base_url)
s.main()
