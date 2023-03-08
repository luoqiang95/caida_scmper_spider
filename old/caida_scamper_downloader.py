# -*- coding: utf-8

"""
@Create Time : 2020/11/3 15:49 
@Authors     : Zhiyong.Zhang
@File Name   : caida_scamper_downloader
@Description : 1. 输入日期（年月日），自动下载CAIDA probe-data数据：
               Examples
               --------
               -s 20191030 -e 20191103 -t probe
               => 自动下载下面链接中的数据
               http://data.caida.org/datasets/topology/ark/ipv4/probe-data/team-1/2019/cycle-20191030/
               http://data.caida.org/datasets/topology/ark/ipv4/probe-data/team-1/2019/cycle-20191031/
               http://data.caida.org/datasets/topology/ark/ipv4/probe-data/team-1/2019/cycle-20191101/
               http://data.caida.org/datasets/topology/ark/ipv4/probe-data/team-1/2019/cycle-20191102/
               http://data.caida.org/datasets/topology/ark/ipv4/probe-data/team-1/2019/cycle-20191103/

               2. 输入年份和月份，自动下载CAIDA prefix-probing数据
               Examples
               --------
               -s 201811 -e 201902 -t prefix
               => 自动下载下面链接中的数据
               http://data.caida.org/datasets/topology/ark/ipv4/prefix-probing/2018/11/
               http://data.caida.org/datasets/topology/ark/ipv4/prefix-probing/2018/12/
               http://data.caida.org/datasets/topology/ark/ipv4/prefix-probing/2019/01/
               http://data.caida.org/datasets/topology/ark/ipv4/prefix-probing/2019/02/
@Modif. List : ----------------------
               Zhiyong.Zhang@20201223
               ----------------------
               添加功能2，自动下载CAIDA prefix-probing数据
"""

import os
import argparse
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup


def getHTMLText(url):
    """
    获取网页的html文档
    """
    try:
        # 获取服务器的响应内容，并设置最大请求时间为30秒
        res = requests.get(url, timeout=30)
        # 判断返回状态码是否为200
        res.raise_for_status()
        # 设置该html文档可能的编码
        res.encoding = res.apparent_encoding
        # 返回网页HTML代码
        return res.text
    except Exception as e:
        print(e.__str__())
        return False


def get_date_list(begin_date, end_date):
    """
    获取起始日期和末尾日期间的所有日期，以列表形式返回
    :param begin_date: str
    :param end_date: str
    :return: list

    Examples
    --------
    get_date_list('20191001', '20191005')
    =>
    ['20191001', '20191002', '20191003', '20191004', '20191005']
    """
    dates = []
    # Get the time tuple : dt
    dt = datetime.strptime(begin_date, "%Y%m%d")
    date = begin_date[:]
    while date <= end_date:
        dates.append(date)
        dt += timedelta(days=1)
        date = dt.strftime("%Y%m%d")
    return dates


def get_month_list(start_month, end_month, sep=''):
    """
    获取起始月份和末尾月份的所有月份，以列表形式返回
    :param start_month: str
    :param end_month: str
    :param sep:
    :return: list

    Examples
    --------
    get_month_list('201810', '201902', sep='.')
    =>
    ['2018.10', '2018.11', '2018.12', '2019.01', '2019.02']
    """
    start_year, start_month = int(start_month[:4]), int(start_month[4:])
    end_year, end_month = int(end_month[:4]), int(end_month[4:])
    months = (end_year - start_year) * 12 + end_month - start_month
    month_range = ['%s%s%s' % (start_year + mon // 12, sep, str(mon % 12 + 1).zfill(2))
                   for mon in range(start_month - 1, start_month + months)]
    return month_range


def wget_from_caida(time_list, task: str):
    if task == 'probe':
        time_unit = '天'
    elif task == 'prefix':
        time_unit = '月'
    else:
        print(f'Unknown task: {task}')
        return

    bad_time_list = []
    for cnt, dt in enumerate(time_list):
        print('\n\n' + '=' * 60)
        print(f'开始下载第{cnt+1}/{len(time_list)}{time_unit}({dt})的数据...')
        print('=' * 60)

        url = ''
        if task == 'probe':
            url = r'http://data.caida.org/datasets/topology/ark/ipv4/probe-data/team-1/%s/cycle-%s/' \
                  % (dt[:4], dt)
        elif task == 'prefix':
            url = r'http://data.caida.org/datasets/topology/ark/ipv4/prefix-probing/%s/%s/' \
                  % (dt[:4], dt[4:])
        html_text = getHTMLText(url)

        if not html_text:
            bad_time_list.append(dt)
            continue

        # 本地当前路径下创建文件目录存放下载结果
        if not os.path.exists(dt):
            os.makedirs(dt)

        # 解析HTML代码
        soup = BeautifulSoup(html_text, 'html.parser')

        # 模糊搜索HTML代码的所有包含href属性的<a>标签
        a_labels = soup.find_all('a', attrs={'href': True})

        # 获取所有<a>标签中的href对应的值，即超链接
        file_name_list = [a.get('href') for a in a_labels if a.get('href').endswith('.warts.gz')]

        for cnt_f, f in enumerate(file_name_list):
            print('=' * 60)
            print(f'开始下载第{cnt+1}/{len(time_list)}{time_unit}({dt})第{cnt_f+1}/{len(file_name_list)}个文件')
            print('=' * 60)

            if os.path.exists(os.path.join(dt, f)):
                print('文件已存在，将直接跳过')
                continue

            source_file_url = url + f
            cmd = 'wget ' + source_file_url

            # 可能由于网络不稳定连接失败，尝试5次
            num_try = 5
            while num_try > 0:
                num_try -= 1
                print('下载命令:\n', cmd)
                os.system(cmd)

                # 本次下载成功，结束循环
                if os.path.exists(f):
                    # 将数据移到当日数据对应的目录
                    os.system('mv %s %s' % (f, dt))
                    break

    with open('bad-times-%d.log' % len(bad_time_list), 'w') as f:
        f.write('\n'.join(bad_time_list))


if __name__ == '__main__':
    # -s 201910 -e 201911 -t prefix
    # -s 20191001 -e 20191031 -t probe
    parser = argparse.ArgumentParser()
    parser.add_argument("-t", dest="task", help="'probe' or 'prefix'", type=str)
    parser.add_argument("-s", dest="start_time", help="data time: first day/month", type=str)
    parser.add_argument("-e", dest="end_time", help="data time: end day/month", type=str)
    args = parser.parse_args()

    time_list = []
    if not args.end_time:
        time_list = [args.start_time]
    else:
        if args.task == 'probe':
            time_list = get_date_list(args.start_time, args.end_time)
        elif args.task == 'prefix':
            time_list = get_month_list(args.start_time, args.end_time)
        else:
            print(f'Unknown task {args.task}. ["probe", "prefix"]')
            parser.print_help()
            exit(-1)

    wget_from_caida(time_list, args.task)
