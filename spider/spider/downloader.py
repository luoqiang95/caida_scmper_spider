# -*- coding:utf-8 -*-
# Author: lq
# data:  下午4:34
# file: downloader.py
import os
import signal
import argparse
import sys
import time

import requests
import pandas as pd
from io import BytesIO
from lxml import etree
from queue import Queue
from logger import init_logger
from datetime import datetime
from db_operation import create_engine
from hdfs_operation import init_hdfs_client
from concurrent.futures import ThreadPoolExecutor

table_name = "caida_file"
table_columns = ["filename", "year", "date", "down_time", "size", "path", "now_size", "over", "url", "modified"]
df = pd.DataFrame(
    columns=table_columns)
engine, conn = create_engine()
queue = Queue()
cpu_count = os.cpu_count()
pool = ThreadPoolExecutor(max_workers=cpu_count)


class ScamperSpider:
    try_count = 10

    _base_url = "https://publicdata.caida.org/datasets/topology/ark/ipv4/probe-data/team-1/"

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
    update_info = {}
    all_over = 0

    def __init__(self, base_path=None, log_path=None, base_url=None, start_time=None, end_time=None, hdfs_url=None,
                 hdfs_user=None):
        self.base_url = self._base_url if not base_url else base_url
        self.down_headers = self.base_headers.copy()
        self.session = requests.session()
        self.file_mapper = dict()  # {"filename":[file_obj,file_obj]}
        self.date_range = pd.date_range(start_time, end_time) if start_time and end_time else None
        self.base_path = base_path if base_path else self.__base_path
        queue.put((self.base_url, None))
        # if not os.path.exists(self.__base_path):
        #     os.mkdir(self.__base_path)
        self.record = None
        self.logger = init_logger(log_path)
        signal.signal(signal.SIGINT, ScamperSpider.ctrl_c)
        self.client = init_hdfs_client(url=hdfs_url, user=hdfs_user)

    @staticmethod
    def ctrl_c(signum, frame):
        if not df.empty:
            df.to_sql(name=table_name, con=engine, index=False, if_exists="append")
        sys.stderr.write("\r\nfinish spider!!")
        sys.exit(0)

    def spider_all_dirs(self):
        """

        """
        n = 0
        while True:
            if n == 5:
                self.all_over += 1
                break
            try:
                url = queue.get(timeout=5)
                self.get_html(url)
            except Exception as t:
                self.logger.error(msg="cant got any link!")
                n += 1

    def get_html(self, _url=None):
        """
        get html source code, use etree parse label
        """
        url, _range = _url
        if url.endswith("warts.gz"):
            file_df = self.record[self.record.url == url]
            if file_df.empty or not file_df.over.values[0]:
                self.get_file_info(url, _range)
        else:
            for i in range(self.try_count):
                try:
                    with self.session.get(url, headers=self.base_headers, timeout=5) as se:
                        html = etree.HTML(se.text, base_url=url)
                    if html is not None:
                        self.parse_html(html)
                    break
                except Exception as e:
                    self.logger.error(msg=f"error! {_url}:")
                    self.logger.error(msg=f"    {str(e)}")

    def parse_html(self, html):
        a_list = html.xpath("//a/@href")
        self.create_dirs_or_file(html.base, a_list[5:])

    def create_dirs_or_file(self, url, a_list, put=True):
        path_suffix = url.replace(self._base_url, '').split("/")[:-1]
        if path_suffix:
            current_path_rel = f"{os.sep}".join(path_suffix)
            path = os.path.join(self.base_path, current_path_rel)
            # if not os.path.exists(path):
            #     os.makedirs(path)
            if not self.client.content(path, strict=False):
                self.client.makedirs(path)
        else:
            path = self.base_path
        for a_u in a_list:
            new_dir = os.path.join(path, a_u)
            # if not os.path.exists(new_dir):
            if not self.client.content(new_dir, strict=False):
                if a_u.endswith("warts.gz"):
                    # os.mknod(new_dir)
                    # self.client.write(new_dir, encoding="utf-8", overwrite=True)
                    self.logger.info(msg=f"create new file {new_dir}")
                else:
                    # os.mkdir(new_dir)
                    self.client.makedirs(new_dir)
                    self.logger.info(msg=f"create new dir {new_dir}")
            if put:
                new_url = url + a_u
                queue.put((new_url, None))

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
            self.logger.info("file link error!!!!")
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
            headers = self.get_download_headers(url, _range)
            for i in range(self.try_count):
                try:
                    with self.session.get(url, headers=headers, timeout=5) as se:
                        if 200 <= se.status_code < 300:
                            self.create_dirs_or_file(url, [filename], False)
                            size, modified = self.get_file_size(se)
                            df.loc[df.filename == filename, "size"] = size
                            df.loc[df.filename == filename, "modified"] = modified
                            if se.headers.get("Content-Range") is not None:
                                capital = size // cpu_count
                                for i in range(cpu_count):
                                    if i == cpu_count - 1:
                                        _range = f"bytes={i * capital}-{size}"
                                    else:
                                        _range = f"bytes={i * capital}-{(i + 1) * capital - 1}"
                                    queue.put((url, _range))
                                return
                            self.create_file_object(se, filename, _range)
                    break
                except Exception as e:
                    self.logger.error(msg=f"download {filename} error!!")
                    self.logger.error(msg=f"{filename} url: {url}")
                    self.logger.error(msg=f"{str(e)}")
        else:
            self.download_warts_file(url, _range)

    def save_file_info(self, url):
        global df
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
            "path": path,
            "over": 0
        }, index=[0])
        if df[df.filename == filename].empty:
            df = pd.concat([df, file_info_df], ignore_index=True)
        else:
            df.loc[df.filename == filename, "date"] = date
            df.loc[df.filename == filename, "year"] = year
            df.loc[df.filename == filename, "url"] = url
            df.loc[df.filename == filename, "down_time"] = datetime.now()
            df.loc[df.filename == filename, "path"] = path

    def download_warts_file(self, url, _range):
        filename = url.split("/")[-1]
        headers = self.get_download_headers(url, _range)
        for i in range(self.try_count):
            try:
                with self.session.get(url, headers=headers, timeout=5) as se:
                    if se.status_code == 206:
                        se = se
                self.create_file_object(se, filename, _range)
                break
            except Exception as e:
                self.logger.error(msg=f"method download_warts_file error!!")
                self.logger.error(msg=f"download {filename} error!!")
                self.logger.error(msg=f"{filename} url: {url}")
                self.logger.error(msg=f"{str(e)}")

    def get_download_record(self):
        global df
        sql = f"SELECT * from {table_name};"
        record = pd.read_sql(sql, conn)
        return record

    def create_file_object(self, session, filename, _range):
        """
        create file object
        """
        file = BytesIO()
        file.write(session.content)
        self.logger.info(msg=f"{filename} number {_range} downloaded")
        self.file_mapper[filename]["file"].append((_range, file))
        self.concat_file_obj(filename)

    def parse_time(self, date: str):
        a = date.replace(",", '').replace(" GMT", '')
        formatter = "%a %d %b %Y %H:%M:%S"
        return datetime.strptime(a, formatter)

    def get_file_size(self, response):
        """
        get file size
        """
        headers = response.headers
        e_tag = headers.get("ETag").replace('"', '')
        size = int("0x" + e_tag.split("-")[0], 16)
        modified = self.parse_time(headers.get("Last-Modified"))
        return size, modified

    def concat_file_obj(self, filename):
        global df
        files = self.file_mapper[filename]["file"]
        files_count = len(files)
        if files_count == cpu_count or files_count == 1:
            objs = sorted(files, key=lambda x: int(x[0].split("-")[-1]) if x[0] else -1)
            path = self.file_mapper[filename]["path"]
            # with open(path, "wb") as fd:
            with self.client.write(path, overwrite=True) as writer:
                for obj_ in objs:
                    writer.write(obj_[1].getvalue())
                    df.loc[df.filename == filename, "now_size"] = int(obj_[0].split("-")[-1]) if obj_[0] else obj_[
                        1].__sizeof__()
                    obj_[1].close()
            df.loc[df.filename == filename, "over"] = 1
            if not self.record[self.record.filename == filename].empty:
                index = df.loc[df.filename == filename].index
                self.update_info[filename] = 1
                df = df.drop(index, axis=0)
            self.logger.info(msg=f"save file in path {path}")

    def to_sql(self):
        global df
        while True:
            if self.all_over == cpu_count:
                self.to_sql_handler()
                break
            time.sleep(10)

    def to_sql_handler(self):
        global df
        fail_files = self.record.loc[self.record.over == 0, "filename"].values
        fail_index = df[df["filename"].isin(fail_files)].index
        df = df.drop(fail_index, axis=0)
        df.to_sql(name=table_name, con=engine, index=False, if_exists="append")
        cur = conn.cursor()
        for k, v in self.update_info.items():
            sql = f"UPDATE {table_name} SET over={v} WHERE filename='{k}';"
            cur.execute(sql)
        conn.commit()
        cur.close()
        conn.close()

    def main(self):
        """
        spider scamper
        """
        self.record = self.get_download_record()
        for i in range(cpu_count):
            pool.submit(self.spider_all_dirs)
        pool.submit(self.to_sql)


def main(base_url=None, base_path=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", dest="save_path", help="warts file save path, default pwd path", type=str, default=None)
    parser.add_argument("-lp", dest="log_path", help="log file save path, default pwd path", type=str, default=None)
    parser.add_argument("-l", dest="link",
                        help="download url, default url: https://publicdata.caida.org/datasets/topology/ark/ipv4/probe-data/team-1/",
                        type=str, default=None)
    parser.add_argument("-s", dest="start_time", help="date time, like:20220301", type=str, default=None)
    parser.add_argument("-e", dest="end_time", help="end time, like:20220302", type=str, default=None)
    parser.add_argument("-ul", dest="hdfs_url", help="hdfs config hdfs url", type=str, default=None)
    parser.add_argument("-us", dest="hdfs_user", help="hdfs config hdfs user", type=str, default=None)
    args = parser.parse_args()
    s = ScamperSpider(base_path=args.save_path, log_path=args.log_path, base_url=args.link, start_time=args.start_time,
                      end_time=args.end_time, hdfs_url=args.hdfs_url, hdfs_user=args.hdfs_user)
    # s = ScamperSpider(base_url=base_url, base_path=base_path)
    s.main()
