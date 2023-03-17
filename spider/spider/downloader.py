# -*- coding:utf-8 -*-
# Author: lq
# data:  下午4:34
# file: downloader.py
import os
import signal
import argparse
import sys

import requests
import pandas as pd
from io import BytesIO
from lxml import etree
from queue import Queue
from logger import init_logger
from datetime import datetime
from db_operation import create_engine
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

    def __init__(self, base_path=None, log_path=None, base_url=None, start_time=None, end_time=None):
        self.base_url = self._base_url if not base_url else base_url
        self.down_headers = self.base_headers.copy()
        self.session = requests.session()
        self.file_mapper = dict()  # {"filename":[file_obj,file_obj]}
        self.date_range = pd.date_range(start_time, end_time) if start_time and end_time else None
        self.base_path = base_path if base_path else self.__base_path
        queue.put((self.base_url, None))
        if not os.path.exists(self.__base_path):
            os.mkdir(self.__base_path)
        self.record = None
        self.logger = init_logger(log_path)

        signal.signal(signal.SIGINT, ScamperSpider.ctrl_c)

    @staticmethod
    def ctrl_c(signum, frame):
        if not df.empty:
            print("exit look:", df)
            df.to_sql(name=table_name, con=engine, index=False, if_exists="append")
        sys.stderr.write("\r\nfinish spider!!")
        sys.exit(0)

    def spider_all_dirs(self):
        """

        """
        n = 0
        while True:
            if n == 5:
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
                    self.logger.info(msg=f"create new file {new_dir}")
                else:
                    os.mkdir(new_dir)
                    self.logger.info(msg=f"create new dir {new_dir}")
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
            print("file link error!!!!")
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
                                    print(f"---------queue put {(url, _range)}")
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
        sql = f"SELECT {','.join(table_columns)} from {table_name};"
        record = pd.read_sql(sql, conn)
        conn.close()
        return record

    def create_file_object(self, session, filename, _range):
        """
        create file object
        """
        file = BytesIO()
        file.write(session.content)
        self.logger.info(msg=f"{filename} number {_range} downloaded")
        self.file_mapper[filename]["file"].append((_range, file))
        print(f"create file success!! ,{_range} {file.__sizeof__()}")
        print(self.file_mapper[filename])
        self.concat_file_obj(filename)

    def get_file_size(self, response):
        """
        get file size
        """
        headers = response.headers
        e_tag = headers.get("ETag").replace('"', '')
        size = int("0x" + e_tag.split("-")[0], 16)
        modified = self.parse_time(headers.get("Last-Modified"))
        return size, modified

    def parse_time(self, date: str):
        a = date.replace(",", '')
        formatter = "%a %d %d %Y %H:%M:%S"
        return datetime.strptime(a, formatter)

    def concat_file_obj(self, filename):
        files = self.file_mapper[filename]["file"]
        if len(files) >= cpu_count:
            objs = sorted(files, key=lambda x: int(x[0].split("-")[-1]))
            path = self.file_mapper[filename]["path"]
            with open(path, "wb") as fd:
                for obj_ in objs:
                    fd.write(obj_[1].getvalue())
                    df.loc[df.filename == filename, "now_size"] = int(obj_[0].split("-")[-1])
            df.loc[df.filename == filename, "over"] = 1
            self.logger.info(msg=f"save file in path {path}")
            print("+" * 5)

    def main(self):
        """
        spider scamper
        """
        self.record = self.get_download_record()
        for i in range(cpu_count):
            pool.submit(self.spider_all_dirs)
        if not df.empty:
            print("look:", df)
            df.to_sql(name=table_name, con=engine, index=False, if_exists="append")


def main(base_url=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", dest="save_path", help="warts file save path, default pwd path", type=str, default=None)
    parser.add_argument("-lp", dest="log_path", help="log file save path, default pwd path", type=str, default=None)
    parser.add_argument("-l", dest="link",
                        help="download url, default url: https://publicdata.caida.org/datasets/topology/ark/ipv4/probe-data/team-1/",
                        type=str, default=None)
    parser.add_argument("-s", dest="start_time", help="date time, like:20220301", type=str, default=None)
    parser.add_argument("-e", dest="end_time", help="end time, like:20220302", type=str, default=None)
    args = parser.parse_args()
    # s = ScamperSpider(base_path=args.save_path, log_path=args.log_path, base_url=args.link, start_time=args.start_time,
    #                   end_time=args.end_time)
    s = ScamperSpider(base_url=base_url)
    s.main()
