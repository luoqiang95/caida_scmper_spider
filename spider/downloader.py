# -*- coding:utf-8 -*-
# Author: lq
# data:  下午4:34
# file: downloader.py
import os
import argparse
from time import sleep

import requests
import pandas as pd
from io import BytesIO
from lxml import etree
from queue import Queue, Empty
from logger import logger
from datetime import datetime
from db_operation import conn
from concurrent.futures import ThreadPoolExecutor


class ScamperSpider:
    try_count = 5
    table_name = "caida_file"
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
        self.queue.put((self.base_url, None))
        if not os.path.exists(self.__base_path):
            os.mkdir(self.__base_path)
        self.record = None

    def spider_all_dirs(self):
        """

        """
        n = 0
        while True:
            if n == 5:
                print("break!!!!")
                break
            try:
                url = self.queue.get(timeout=5)
                self.get_html(url)
            except Empty as t:
                logger.error(msg="cant got any link!")
                print("cant got any link!")
                n += 1

    def get_html(self, _url=None):
        """
        get html source code, use etree parse label
        """
        url, _range = _url
        if url.endswith("warts.gz"):
            self.get_file_info(url, _range)
        else:
            for i in range(self.try_count):
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
            print("get new url: ", new_url)
            self.queue.put((new_url, None))

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
        return self.down_headers.copy()

    def get_file_info(self, url, _range=None):
        if not url.endswith(".warts.gz"):
            print("file link error!!!!")
            return
        file_df = self.record[self.record.url == url]
        if not file_df.empty:
            return
        path_suffix = url.replace(self._base_url, '').split("/")
        path = os.path.join(self.base_path, f'{os.sep}'.join(path_suffix))
        url_split = url.split("/")
        filename = url_split[-1]
        date = url_split[-2].split("-")[-1]
        if self.date_range and date not in self.date_range:
            return
        self.save_file_info(url)
        self.file_mapper[filename] = {"path": path, "file": []}
        if _range is None:
            for i in range(self.try_count):
                try:
                    with self.session.get(url, headers=self.base_headers, timeout=5) as se:
                        if 200 <= se.status_code < 300:
                            size, modified = self.get_file_size(se)
                            self.df.loc[self.df.filename == filename, "size"] = size
                            self.df.loc[self.df.filename == filename, "modified"] = modified
                            if se.headers.get("Content-Range") is not None:
                                capital = size // self.cpu_count
                                for i in range(self.cpu_count):
                                    if i == self.cpu_count - 1:
                                        _range = f"bytes={i * capital}-{size}"
                                    else:
                                        _range = f"bytes={i * capital}-{(i + 1) * capital - 1}"
                                    self.queue.put((url, _range))
                                return
                            self.create_file_object(se, filename, _range)
                except Exception as e:
                    logger.error(msg=f"download {filename} error!!")
                    logger.error(msg=f"{filename} url: {url}")
                    logger.error(msg=f"{str(e)}")
        else:
            self.download_warts_file(url, _range)

    def save_file_info(self, url):
        url_split = url.split("/")
        filename = url_split[-1]
        date = url_split[-2].split("-")[-1]
        year = url_split[-3]
        path_suffix = url.replace(self._base_url, '').split("/")
        path = os.path.join(self.base_path, f'{os.sep}'.join(path_suffix))
        file_info_df = pd.DataFrame({
            "filename": filename,
            "date": date,
            "year": year,
            "url": url,
            "down_time": datetime.now(),
            "path": path
        }, index=[0])
        if self.df[self.df.filename == filename].empty:
            self.df = pd.concat([self.df, file_info_df], ignore_index=True)
        self.df.loc[self.df.filename == filename, "date"] = date
        self.df.loc[self.df.filename == filename, "year"] = year
        self.df.loc[self.df.filename == filename, "url"] = url
        self.df.loc[self.df.filename == filename, "down_time"] = datetime.now()
        self.df.loc[self.df.filename == filename, "path"] = path

    def download_warts_file(self, url, _range):
        filename = url.split("/")[-1]
        headers = self.get_download_headers(url, _range)
        for i in range(self.try_count):
            try:
                with self.session.get(url, headers=headers, timeout=5) as se:
                    if se.status_code == 206:
                        se = se
                self.create_file_object(se, filename, _range)
            except Exception as e:
                logger.error(msg=f"method download_warts_file error!!")
                logger.error(msg=f"download {filename} error!!")
                logger.error(msg=f"{filename} url: {url}")
                logger.error(msg=f"{str(e)}")

    def get_download_record(self):
        sql = f"SELECT {','.join(self.table_columns)} from {self.table_name};"
        record = pd.read_sql(sql, conn)
        return record

    def create_file_object(self, session, filename, _range):
        """
        create file object
        """
        file = BytesIO()
        file.write(session.content)
        logger.info(msg=f"{filename} number {_range} downloaded")
        self.file_mapper[filename]["file"].append((_range, file))

    def get_file_size(self, response):
        """
        get file size
        """
        headers = response.headers
        e_tag = headers.get("ETag").replace('"', '')
        size = int("0x" + e_tag.split("-")[0], 16)
        modified = headers.get("Last-Modified")
        return size, modified

    def concat_file_obj(self):
        for filename, obj in self.file_mapper.items():
            file_objs = obj["file"]
            objs = sorted(file_objs, key=lambda x: int(x[0].split("-")[-1]))
            path = obj["path"]
            with open(path, "wb") as fd:
                for obj_ in objs:
                    fd.write(obj_[1].read())
                    self.df.loc[self.df.filename == filename, "now_size"] = int(obj_[0].split("-")[-1])
            self.df.loc[self.df.filename == filename, "over"] = 1
            logger.info(msg=f"save file in path {path}")

    def main(self):
        """
        spider scamper
        """
        self.record = self.get_download_record()
        for i in range(self.cpu_count):
            self.pool.submit(self.spider_all_dirs)
        # self.spider_all_dirs()
        if not self.df.empty:
            self.df.to_sql(self.table_name, conn, index=False, if_exists="append")


def main():
    # base_url = "https://publicdata.caida.org/datasets/topology/ark/ipv4/probe-data/team-1/2022/cycle-20220302/"
    # parser = argparse.ArgumentParser()
    # parser.add_argument("-p", dest="save_path", help="warts file save path, default pwd path", type=str, default=None)
    # parser.add_argument("-l", dest="link",
    #                     help="download url, default url: https://publicdata.caida.org/datasets/topology/ark/ipv4/probe-data/team-1/",
    #                     type=str, default=None)
    # parser.add_argument("-s", dest="start_time", help="date time, like:20220301", type=str, default=None)
    # parser.add_argument("-e", dest="end_time", help="end time, like:20220302", type=str, default=None)
    # args = parser.parse_args()

    # s = ScamperSpider(base_path=args.save_path, base_url=args.link, start_time=args.start_time, end_time=args.end_time)
    s = ScamperSpider(
        base_url="https://publicdata.caida.org/datasets/topology/ark/ipv4/probe-data/team-1/2022/cycle-20220302/")
    s.main()


if __name__ == '__main__':
    main()
