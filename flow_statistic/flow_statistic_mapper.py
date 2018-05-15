#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Created on 5/10/18 2:21 PM
@author: Chen Liang
@function: mapper
"""

import sys
import os
import click
import re
import urllib
import time
import datetime


line_parser = None


class LogLineParser(object):
    def __init__(self):
        pass

    def parse(self, cdn_code, protocol, line, log_name):
        parse_func = self.build_parse_fun(protocol, cdn_code)
        res = parse_func(line)
        if res is not None:
            _, ip, live_id, _, _, up_flow, down_flow, _ = parse_func(line)
            seconds = self.build_timestamp(cdn_code, log_name)
            if seconds is not None:
                # datetime_str = self.second_to_datetime_string(int(start_second) + int(seconds))
                # user_id = str(self.live_info.look_up(int(live_id)))
                formatted_line = '\t'.join([live_id, ip, up_flow, down_flow])
                return formatted_line
        return None

    def build_parse_fun(self, protocol, cdn_code):
        """
        根据protocol, cdn_code构造解析函数
        :param protocol: 协议 hls | hdl | rtmp
        :param cdn_code: netcenter | alicdn | dnion | tencent
        :return: 返回解析函数
        """
        def parse_func(line):
            if protocol == 'hls':
                method = getattr(self, 'parse_http_line_{}'.format(cdn_code))
                return method(line)
            elif protocol == 'rtmp':
                method = getattr(self, 'parse_rtmp_line_{}'.format(cdn_code))
                return method(line)
            elif protocol == 'hdl':
                method = getattr(self, 'parse_hdl_line_{}'.format(cdn_code))
                return method(line)
            else:
                method = getattr(self, 'parse_http_line_{}'.format(cdn_code))
                return method(line)
        return parse_func

    def build_timestamp(self, cdn_code, log_name):
        if cdn_code == 'netcenter':
            return self.build_netcenter_timestamp(log_name)
        elif cdn_code == 'dnion':
            return self.build_dnion_timestamp(log_name)
        elif cdn_code == 'alicdn':
            return self.build_alicdn_timestamp(log_name)
        elif cdn_code == 'tencent':
            return self.build_tencent_timestamp(log_name)
        else:
            return self.build_netcenter_timestamp(log_name)

    def build_netcenter_timestamp(self, log_name):
        """根据netcenter日志名称获取日志内容日期的date对应的timestamp"""
        # example: 2017-12-06-2300-2330_rtmp-wsz.qukanvideo.com.cn.log.gz
        date_str = log_name[:10]
        try:
            dt = datetime.datetime.strptime("{} 0:0:0".format(date_str), "%Y-%m-%d %H:%M:%S")
            seconds = self.datetime_to_gregorian_seconds(dt)
            return seconds
        except (ValueError, Exception):
            return

    def build_dnion_timestamp(self, log_name):
        """根据dnion日志名称获取日志内容日期的date对应的timestamp"""
        # example: hls-d.quklive.com_20180509_03_04.gz
        try:
            date_str = log_name.split('_')[1]
            dt = datetime.datetime.strptime("{} 0:0:0".format(date_str), "%Y%m%d %H:%M:%S")
            seconds = self.datetime_to_gregorian_seconds(dt)
            return seconds
        except (IndexError, ValueError, Exception):
            return

    def build_alicdn_timestamp(self, log_name):
        """根据alicdn日志名称获取日志内容日期的date对应的timestamp"""
        # example: play-a.quklive.com_2017_12_07_1100_1200.gz
        try:
            lst = log_name.split('_')
            date_str = '{}{}{}'.format(lst[1], lst[2], lst[3])
            dt = datetime.datetime.strptime("{} 0:0:0".format(date_str), "%Y%m%d %H:%M:%S")
            seconds = self.datetime_to_gregorian_seconds(dt)
            return seconds
        except (IndexError, ValueError, Exception):
            return

    def build_tencent_timestamp(self, log_name):
        """根据tencent日志名称获取日志内容日期的date对应的timestamp"""
        # example: 2017120607_hangzhouqukan.cdn.log.gz
        date_str = log_name[:8]
        try:
            dt = datetime.datetime.strptime("{} 0:0:0".format(date_str), "%Y%m%d %H:%M:%S")
            seconds = self.datetime_to_gregorian_seconds(dt)
            return seconds
        except (ValueError, Exception):
            return

    @staticmethod
    def second_to_datetime_string(seconds):
        """
        将从公元0年开始的秒数转换为datetime的string形式
        :param seconds: 从公元0年开始的秒数
        :return: datetime的string形式
        """
        # s = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(float(seconds)))
        # print s
        # year = s.split('-', 1)[0]
        # rest = s.split('-', 1)[1]
        # year = int(year) - 1970  # datetime是从1970开始的，因此计算时需要减去1970
        # return '{}-{}'.format(str(year), rest)
        # datetime是从1970开始的，因此计算时需要减去{{1970, 1, 1}, {0, 0, 0}}对应的秒数62167219200
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(float(seconds - 62167219200 + 86400 - 28800)))

    def datetime_to_gregorian_seconds(self, dt):
        """
        获取从公元0年1月1日开始到当天0点所经过的秒数
        :param dt: datetime.datetime类型
        :return: 返回从公元0年1月1日开始到当天0点所经过的秒数
        """
        d = dt.date()
        t = dt.time()
        # toordinal 从1年1月1日开始, erlang 的datetime_to_gregorian_seconds和date_to_gregorian_days从0年1月1日开始
        # 当天不算所以需要减1天
        return (d.toordinal() + 365 - 1) * 86400 + self.time_to_second(t.hour, t.minute, t.second)

    @staticmethod
    def time_to_second(time_h, time_m, time_s):
        """
        根据给定的time_h, time_m, time_s计算当天已过去的时间，秒为单位
        :param time_h: 小时
        :param time_m: 分
        :param time_s: 秒
        :return: 返回计算的second
        """
        return int(time_h) * 3600 + int(time_m) * 60 + int(time_s)

    @staticmethod
    def utc_time_to_second(utc_time):
        """
        根据给定的utc_time计算当天已过去的时间，秒为单位(需要转化同一个时区，默认东8区)
        :param utc_time: utc时间戳，类似1464830584
        :return: 返回计算的second
        """
        t = datetime.datetime.fromtimestamp(int(utc_time))
        return (t.hour - 8) * 3600 + t.minute * 60 + t.second

    @staticmethod
    def getdomain(url):
        """
        获取url中的域名，带host
        :param url: 完整url，例如https://develop.aliyun.com/tools/sdk?#/python
        :return: 返回url中带host的域名，例如develop.aliyun.com
        """
        proto, rest = urllib.splittype(url)
        host, rest = urllib.splithost(rest)
        return host if host is not None else url

    @staticmethod
    def get_timestamp_0clock(timestamp):
        t = time.localtime(timestamp)
        time1 = time.mktime(time.strptime(time.strftime('%Y-%m-%d 00:00:00', t), '%Y-%m-%d %H:%M:%S'))
        return int(time1)

    @staticmethod
    def getdomain(url):
        """
        获取url中的域名，带host
        :param url: 完整url，例如https://develop.aliyun.com/tools/sdk?#/python
        :return: 返回url中带host的域名，例如develop.aliyun.com
        """
        proto, rest = urllib.splittype(url)
        host, rest = urllib.splithost(rest)
        return host if host is not None else url

    def parse_http_line_netcenter(self, line):
        """
        将line和(netcenter，http, live)进行匹配
        :param line: 示例：s = '10.73.23.77 - - [21/Aug/2017:23:58:20 +0800] "GET http://hls-w.quklive.com/live/w1503292073771758/playlist.m3u8 HTTP/1.1" 200 551 "http://m.thepaper.cn/newsDetail_forward_1769793?from=timeline&isappinstalled=1" "Mozilla/5.0 (iPhone; CPU iPhone OS 8_1_1 like Mac OS X) AppleWebKit/600.1.4 (KHTML, like Gecko) Mobile/12B436 MicroMessenger/6.5.3 NetType/WIFI Language/zh_CN" 246'
        :return: 返回抓取之后格式化的结果,没有匹配则返回None
        """
        pattern = '''^(.*?\\b)\\s.*?/\\d*:(\\d{2}):(\\d{2}):(\\d{2}\\b)\\s.*\\].*/(wtest_|test_|w|ba|bp)(\\d{5,})/.*\..*\\sHTTP.*\\s2\\d{2}\\s(\\d+)\\s"(.*)"\\s.*\\s\\d+$'''
        p = re.compile(pattern)
        m = p.match(line)
        if m:
            ip, time_h, time_m, time_s, _, live_id, down_flow, referrer = m.groups()
            start_second = self.time_to_second(time_h, time_m, time_s)
            return 'hls', ip, live_id, start_second, '0', '0', down_flow, self.getdomain(referrer)
        return None

    def parse_http_line_dnion(self, line):
        """
        将line和(dnion，http, live)进行匹配
        :param line: 示例：s = '220.184.255.156 - - [21/Aug/2017:10:29:49 +0800] "GET http://hls-d.quklive.com/qklive-sz/d1503281285294904/index.m3u8 HTTP/HTTP/1.1.-" 200 971 "http://cloud.quklive.com/cloud/static/jwplay.html?src=http%3A%2F%2Fhls-d.quklive.com%2Fqklive-sz%2Fd1503281285294904%2Findex.m3u8&record=&skin=seven" "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36"'
        :return: 返回抓取之后格式化的结果,没有匹配则返回None
        """
        pattern = '''^(.*?\\b)\\s.*?/\\d*:(\\d{2}):(\\d{2}):(\\d{2}\\b)\\s.*\].*/(dtest_|test_|d|ba|bp)(\\d*).*\..*\\sHTTP.*\\s2\\d{2}\\s(\\d*)\\s"(.*)"\\s.*$'''
        p = re.compile(pattern)
        m = p.match(line)
        if m:
            ip, time_h, time_m, time_s, _, live_id, down_flow, referrer = m.groups()
            start_second = self.time_to_second(time_h, time_m, time_s)
            return 'hls', ip, live_id, start_second, '0', '0', down_flow, self.getdomain(referrer)
        return None

    def parse_http_line_alicdn(self, line):
        """
        将line和(alicdn，http, live)进行匹配
        :param line: 示例：s = '[11/Jul/2017:10:58:58 +0800] 220.184.252.13 - 42 "http://cloud.quklive.com/cloud/static/jwplayer/jwplayer.flash.swf" "GET http://play-a.quklive.com/live/a1499736105668939.m3u8" 200 591 1134 MISS "Mozilla/5.0 (Windows NT 6.3; WOW64; Trident/7.0; rv:11.0) like Gecko" "application/x-mpegURL"'
        :return: 返回抓取之后格式化的结果,没有匹配则返回None
        """
        # pattern = '''^\\[.*?/\\d{4}:(\\d{2}):(\\d{2}):(\\d{2}\\b)\\s.*\\]\\s(.*?)\\s.*http://.*/\\D+(/a|_a)(\\d{5,}).*?\\s2\\d+\\s\\d+\\s(\\d+).*$'''
        pattern = '''^\\[.*?/\\d{4}:(\\d{2}):(\\d{2}):(\\d{2}\\b)\\s.*\\]\\s(.*?)\\s-\\s\\d+\\s"(.*)"\\s.*http://.*/\\D+(/a|_a)(\\d{5,}).*?\\s2\\d+\\s\\d+\\s(\\d+).*$'''
        p = re.compile(pattern)
        m = p.match(line)
        if m:
            time_h, time_m, time_s, ip, referrer, _, live_id, down_flow = m.groups()
            start_second = self.time_to_second(time_h, time_m, time_s)
            return 'hls', ip, live_id, start_second, '0', '0', down_flow, self.getdomain(referrer)
        return None

    def parse_http_line_tencent(self, line):
        """
        将line和(tencent，http, live)进行匹配
        :param line: s = '125.119.247.229	1512578208	t1512552599083933-1512556009	0	571	521	1270990	http://cloud.quklive.com/cloud/static/jwplay.html?src%3Dhttp%253A%252F%252Fhls-t.qukanvideo.com%252Flive%252Ft1512552599083933.m3u8%26record%3D%26skin%3Dseven'
        :return: 返回抓取之后格式化的结果,没有匹配则返回None
        """
        pattern = '''^(.*?\\b)\\s(\\d*)\\s[A-Za-z_]*(\\d+).*\\s(\\d+)\\s(\\d+)\\s(\\d+)\\s(\\d+)\\s(.*)$'''
        p = re.compile(pattern)
        m = p.match(line)
        if m:
            ip, seconds, live_id, _push_time, duration, up_flow, down_flow, referrer = m.groups()
            start_second = int(seconds) - self.get_timestamp_0clock(int(seconds))
            return 'hls', ip, live_id, start_second, duration, up_flow, down_flow, self.getdomain(referrer)
        return None

    def parse_rtmp_line_netcenter(self, line):
        """
        将line和(netcenter，rtmp, live)进行匹配
        :param line: 示例：s = 'stop 220.184.151.202 2016-06-18 17:12:22 - rtmp-wsz.qukanvideo.com live-sz w9466240298191931 606995 200 rtmp - rtmp://rtmp-wsz.qukanvideo.com/live-sz /live-sz - - - - - - - - 69240821 4434 rtmp-73e16c342bcb4ab15c565815-462079'
        :return: 返回抓取之后格式化的结果,没有匹配则返回None
        """
        pattern = '''^(\\w+)\\b\\s(.*?)\\s[0-9-]+\\s(\\d{2}):(\\d{2}):(\\d{2}).*rtmp.*?\\s(wtest_|test_|w|ba|bp)(\\d{5,}).*?\\s(\\d+).*?\\s2\\d{2}\\s.*\\s+(\\d+)\\s+(\\d+)\\s+.*rtmp.*$'''
        p = re.compile(pattern)
        m = p.match(line)
        if m:
            _, ip, time_h, time_m, time_s, _, live_id, duration, up_flow, down_flow = m.groups()
            end_second = self.time_to_second(time_h, time_m, time_s)
            duration_1 = int(round(int(duration)/1000))
            referrer = 'rtmp'
            return 'rtmp', ip, live_id, int(end_second)-int(duration_1), duration_1, up_flow, down_flow, self.getdomain(referrer)
        return None

    def parse_rtmp_line_alicdn(self, line):
        """
        将line和(alicdn，rtmp, live)进行匹配
        :param line: 示例：s = '[3/Jul/2017:18:11:53 +0800] 101.71.217.57 - 5000 "-" "GET rtmp://play-a.quklive.com/live//a1499063507234972?" 200 1995 620999 HIT "" " "'
        :return: 返回抓取之后格式化的结果,没有匹配则返回None
        """
        pattern = '''^\\[.*?/\\d{4}:(\\d{2}):(\\d{2}):(\\d{2}\\b)\\s.*\\]\\s(.*?)\\s.*\\s(\\d+)\\s"(.*)"\\s.*rtmp://.*/\\D+(/a|/atest_|/test)(\\d{5,}).*?\\s2\\d{2}\\s+(\\d+)\\s+(\\d+)\\s+.*$'''
        p = re.compile(pattern)
        m = p.match(line)
        if m:
            time_h, time_m, time_s, ip, duration, referrer, _, live_id, up_flow, down_flow = m.groups()
            end_second = self.time_to_second(time_h, time_m, time_s)
            duration_1 = int(round(int(duration) / 1000))
            return 'rtmp', ip, live_id, end_second - duration_1, duration_1, up_flow, down_flow, self.getdomain(referrer)
        return None

    def parse_rtmp_line_dnion(self, line):
        """
        将line和(dnion，rtmp, live)进行匹配
        :param line: 示例：s = '115.206.160.154 1503378421      -       -       000     -       794679242       PUBLISH rtmp://rtmp-d.quklive.com/qklive-sz     d1503372119064926       -       -       -       -       -       -       FMLE/3.0 (compatible; FMSc/1.0) 6250120 -       -       -       - '
        :return: 返回抓取之后格式化的结果,没有匹配则返回None
        """
        pattern = '''^(.*?\\b)\\s+(\\d{10})\\s+.*\\d+\\s+-\\s+(\\d+).*rtmp://.*?\\s(dtest_|test_|d|ba|bp)(\\d{5,}).*\\s(\\d+)\\s.*$'''
        p = re.compile(pattern)
        m = p.match(line)
        if m:
            # print line
            ip, utc_stamp, down_flow, _, live_id, duration = m.groups()
            end_second = self.utc_time_to_second(utc_stamp)
            duration_1 = int(round(int(duration) / 1000))
            referrer = 'rtmp'
            return 'rtmp', ip, live_id, end_second - duration_1, duration_1, '0', down_flow, self.getdomain(referrer)
        return None

    def parse_rtmp_line_tencent(self, line):
        """
        将line和(tencent，rtmp, live)进行匹配
        :param line: 125.119.247.229	1512641523	t1512639815081947				39294927
        :return: 返回抓取之后格式化的结果,没有匹配则返回None
        """
        pattern = '''^(.*?\\b)\\s+(\\d*)\\s+[A-Za-z_]*(\\d+)\\s\\s\\s\\s(\\d+)(.*)$'''
        p = re.compile(pattern)
        m = p.match(line)
        if m:
            ip, seconds, live_id, down_flow, refer = m.groups()
            start_second = int(seconds) - self.get_timestamp_0clock(int(seconds))
            return 'rtmp', ip, live_id, start_second, '0', '0', down_flow, 'rtmp' if refer == '' else refer
        return None

    def parse_hdl_line_netcenter(self, line):
        """
        将line和(netcenter，hdl, live)进行匹配
        :param line: 示例：s = '220.184.255.156 - - [21/Aug/2017:14:20:46 +0800] "GET http://hdl-wsz.qukanvideo.com/live-sz/w1503288070284956.flv? HTTP/1.1" 200 44688656 "http://cloud.quklive.com/cloud/static/jwplay.html?src=http%3A%2F%2Fhdl-wsz.qukanvideo.com%2Flive-sz%2Fw1503288070284956.flv%0A&record=&skin=seven" "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36" 321414'
        :return: 返回抓取之后格式化的结果,没有匹配则返回None
        """
        pattern = '''^(.*?\\b)\\s.*?/\\d*:(\\d{2}):(\\d{2}):(\\d{2}\\b)\\s.*\\]\\s.*/(wtest_|test_|w|ba|bp)(\\d{5,})\..*HTTP.*\\s2\\d{2}\\s(\\d*)\\s"(.*)"\\s.*\\s(\\d+)$'''
        p = re.compile(pattern)
        m = p.match(line)
        if m:
            ip, time_h, time_m, time_s, _, live_id, down_flow, referrer, duration = m.groups()
            end_second = self.time_to_second(time_h, time_m, time_s)
            duration_1 = int(round(int(duration) / 1000))
            return 'hdl', ip, live_id, end_second - duration_1, duration_1, '0', down_flow, self.getdomain(referrer)
        return None

    def parse_hdl_line_dnion(self, line):
        """
        将line和(dnion，hdl, live)进行匹配
        :param line: 示例：s = '220.184.255.156 - - [21/Aug/2017:10:29:48 +0800] "GET http://hdl-d.quklive.com:80/qklive-sz/d1503281285294904?- HTTP/-.-" 200 6358576 "http://cloud.quklive.com/cloud/static/jwplay.html?src=http%3A%2F%2Fhdl-d.quklive.com%2Fqklive-sz%2Fd1503281285294904.flv&record=&skin=seven" "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36"'
        :return: 返回抓取之后格式化的结果,没有匹配则返回None
        """
        pattern = '''^(.*?\\b)\\s.*?/\\d*:(\\d{2}):(\\d{2}):(\\d{2}\\b)\\s.*\].*/(dtest_|test_|d|ba|bp)(\\d{5,}).*HTTP.*\\s2\\d{2}\\s(\\d*)\\s"(.*)"\\s.*$'''
        p = re.compile(pattern)
        m = p.match(line)
        if m:
            ip, time_h, time_m, time_s, _, live_id, down_flow, referrer = m.groups()
            start_second = self.time_to_second(time_h, time_m, time_s)
            return 'hdl', ip, live_id, start_second, '0', '0', down_flow, self.getdomain(referrer)
        return None

    def parse_hdl_line_alicdn(self, line):
        """
        将line和(alicdn，hdl, live)进行匹配
        :param line: 示例：s = '[11/Jul/2017:09:30:25 +0800] 220.184.252.13 - 5000 "http://cloud.quklive.com/cloud/static/jwplay.html?src=http%3A%2F%2Fplay-a.quklive.com%2Flive%2Fa1499736105668939.flv&record=&skin=seven" "GET http://play-a.quklive.com/live/a1499736105668939.flv" 200 580 250105 - "Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36" "video/x-flv"'
        :return: 返回抓取之后格式化的结果,没有匹配则返回None
        """
        pattern = '''^\\[.*?/\\d{4}:(\\d{2}):(\\d{2}):(\\d{2}\\b)\\s.*\\]\\s(.*?)\\s-\\s\\d+\\s"(.*)"\\s.*http://.*/\\D+/a(\\d{5,}).*?\\s2\\d+\\s\\d+\\s(\\d+).*$'''
        p = re.compile(pattern)
        m = p.match(line)
        if m:
            time_h, time_m, time_s, ip, referrer, live_id, down_flow = m.groups()
            start_second = self.time_to_second(time_h, time_m, time_s)
            return 'hdl', ip, live_id, start_second, '0', '0', down_flow, self.getdomain(referrer)
        return None


def init():
    """初始化"""
    global line_parser
    line_parser = LogLineParser()


def read_input(std_input):
    """生成器：读取输入"""
    for line in std_input:
        yield line


def flow_statistic_mapper(cdn_code, protocol):
    data = read_input(sys.stdin)
    print '{}\t{}\t{}\t{}'.format(os.environ['mapreduce_map_input_file'], '119.23.140.219', 447, 3908)
    for line in data:
        input_file_path = os.environ['mapreduce_map_input_file']
        # input_file_path = "/tmp/2018-05-09-0000-0030_rtmpdist-wsz.qukanvideo.com.cn.log"
        res = line_parser.parse(cdn_code, protocol, line.strip(), input_file_path.split('/')[-1])
        if res is not None:
            print res
        # print '{}\t{}\t{}\t{}'.format(1509533602855987, '119.23.140.219', 447, 3908)


@click.command()
@click.option('--cdn_code', default='netcenter', help='supported cdn code: netcenter|dnion|alicdn|tencent')
@click.option('--protocol', default='rtmp', help='supported protocol: rtmp|hdl|hls')
def run(cdn_code, protocol):
    flow_statistic_mapper(cdn_code, protocol)


if __name__ == '__main__':
    init()
    run()
