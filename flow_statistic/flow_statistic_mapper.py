#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Created on 5/10/18 2:21 PM
@author: Chen Liang
@function: mapper
"""

import sys
import os
# import click
import re
import urllib
import time
import datetime


# 域名和协议对照字典，便于从域名查询属于什么协议
domain_protocol_dict = {
    'rtmp-wsz.qukanvideo.com': ('netcenter', 'live', 'rtmp'),
    'rtmpdist-wsz.qukanvideo.com': ('netcenter', 'live', 'rtmpdist'),
    'hdl-wsz.qukanvideo.com': ('netcenter', 'live', 'hdl'),
    'hls-wsz.qukanvideo.com': ('netcenter', 'live', 'hls'),
    'rtmp-w.quklive.com': ('netcenter', 'live', 'rtmp'),
    'rtmpdist-w.quklive.com': ('netcenter', 'live', 'rtmpdist'),
    'hdl-w.quklive.com': ('netcenter', 'live', 'hdl'),
    'hls-w.quklive.com': ('netcenter', 'live', 'hls'),

    'rtmpdist-d.quklive.com': ('dnion', 'live', 'rtmpdist'),
    'hdl-d.quklive.com': ('dnion', 'live', 'hdl'),
    'hls-d.quklive.com': ('dnion', 'live', 'hls'),

    'play-a.quklive.com': ('alicdn', 'live', 'hls'),
    'recordcdn-sz.qukanvideo.com': ('qukan', 'record', 'hls'),
    'recordcdn.quklive.com': ('qukan', 'record', 'hls'),
}

line_parser = None


class LogLineParser(object):
    def __init__(self):
        pass

    # date_str, cdn_code, type_code, protocol, line.strip()
    def parse(self, date_str, cdn_code, type_code, protocol, line):
        parse_func = self.build_parse_fun(cdn_code, type_code, protocol)
        res = parse_func(line)
        if res is not None:
            _, ip, live_id, start_second, _, up_flow, down_flow, _ = parse_func(line)
            seconds = self.build_timestamp(date_str)
            if seconds is not None:
                datetime_str = self.second_to_datetime_string(int(start_second) + int(seconds))
                formatted_line = '\t'.join([live_id, datetime_str, ip, up_flow, down_flow])
                return formatted_line
        return None

    def build_parse_fun(self, cdn_code, type_code, protocol):
        """
        根据protocol, type_of, cdn_code构造解析函数
        :param protocol: 协议 hls | hdl | rtmp | rtmpdist
        :param type_code: live | record | vod
        :param cdn_code: netcenter | alicdn | dnion | qukan
        :return: 返回解析函数
        """
        def parse_fun(line):
            if protocol == 'hls':
                method = getattr(self, 'parse_http_line_{}_{}'.format(cdn_code, type_code))
                return method(line)
            elif protocol == 'rtmp':
                method = getattr(self, 'parse_rtmp_line_{}_{}'.format(cdn_code, type_code))
                return method(line)
            elif protocol == 'rtmpdist':
                method = getattr(self, 'parse_rtmp_line_{}_{}'.format(cdn_code, type_code))
                return method(line)
            elif protocol == 'hdl':
                method = getattr(self, 'parse_hdl_line_{}_{}'.format(cdn_code, type_code))
                return method(line)
            else:
                method = getattr(self, 'parse_http_line_{}_{}'.format(cdn_code, type_code))
                return method(line)
        return parse_fun

    def build_timestamp(self, date_str):
        try:
            dt = datetime.datetime.strptime("{} 0:0:0".format(date_str), "%Y-%m-%d %H:%M:%S")
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

    def parse_http_line_netcenter_live(self, line):
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

    def parse_http_line_dnion_live(self, line):
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

    def parse_http_line_alicdn_live(self, line):
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

    def parse_http_line_tencent_live(self, line):
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

    def parse_http_line_qukan_live(self, line):
        """
        将line和(qukan，http, live)进行匹配
        :param line: 示例：s = '[2/Jun/2016:11:39:29 +0800] 119.147.146.189 - 301 "-" "GET http://hlscdn-sz.qukanvideo.com/broadcast/activity/1464838337479910/1464838337479910-1464838716031-9.ts" 200 551 445040 MISS "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95  Safari/537.36" "video/MP2T"'
        :return: 返回抓取之后格式化的结果,没有匹配则返回None
        """
        pattern = '''^\\[.*?/\\d{4}:(\\d{2}):(\\d{2}):(\\d{2}\\b)\\s.*\\]\\s(.*?)\\s-\\s\\d+\\s"(.*)"\\s.*http://.*/\\D+/(\\d{5,})/.*?\\s2\\d+\\s\\d+\\s(\\d+).*$'''
        p = re.compile(pattern)
        m = p.match(line)
        if m:
            time_h, time_m, time_s, ip, referrer, live_id, down_flow = m.groups()
            start_second = self.time_to_second(time_h, time_m, time_s)
            return 'hls', ip, live_id, start_second, '0', '0', down_flow, self.getdomain(referrer)
        return None

    def parse_http_line_qukan_record(self, line):
        """
        将line和(qukan，http, record)进行匹配
        :param line: 示例：s = '[12/Dec/2016:11:59:56 +0800] 223.74.154.173 - 505 "_" "GET http://recordcdn.quklive.com/broadcast/activity/1481417072020935/20161211/103348_535.ts" 206 422 1066762 MISS "Mozilla/5.0 (Linux; U; Android 4.4.4; zh-cn; HM NOTE 1S Build/KTU84P) AppleWebKit/533.1 (KHTML, like Gecko) Mobile Safari/533.1" "video/MP2T"'
        :return: 返回抓取之后格式化的结果,没有匹配则返回None
        """
        try:
            is_status_start_with_2 = line.split(' ')[8].startswith('2')
        except IndexError:
            return None
        if is_status_start_with_2:
            pattern = '''^\\[.*?/\\d{4}:(\\d{2}):(\\d{2}):(\\d{2}\\b)\\s.*\\]\\s(.*?)\\s-\\s\\d+\\s"(.*)"\\s.*http://.*/\\D+/(\\d{5,})/.*?\\s2\\d+\\s\\d+\\s(\\d+).*$'''
            p = re.compile(pattern)
            m = p.match(line)
            if m:
                time_h, time_m, time_s, ip, referrer, live_id, down_flow = m.groups()
                start_second = self.time_to_second(time_h, time_m, time_s)
                # print line.rstrip('\n')
                # print time_h, time_m, time_s, ip, live_id, start_second
                return 'hls', ip, live_id, start_second, '0', '0', down_flow, self.getdomain(referrer)
        return None

    def parse_http_line_qukan_vod(self, line):
        """
        将line和(qukan，http, vod)进行匹配
        :param line: 示例：s = '[12/Dec/2016:11:59:57 +0800] 60.186.191.104 - 147 "_" "GET http://recordcdn.quklive.com/upload/vod/user1479287223194964/1480324935768978/1/video-00269.ts" 200 519 1236679 HIT "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:50.0) Gecko/20100101 Firefox/50.0" "application/octet-stream"'
        :return: 返回抓取之后格式化的结果,没有匹配则返回None
        """
        try:
            is_status_start_with_2 = line.split(' ')[8].startswith('2')
        except IndexError:
            return None
        if is_status_start_with_2:
            pattern = '''^\\[.*?/\\d{4}:(\\d{2}):(\\d{2}):(\\d{2}\\b)\\s.*\\]\\s(.*?)\\s-\\s\\d+\\s"(.*)"\\s.*http://.*/\\D+/vod/user(\\d{5,})/(\\d{5,})/.*?\\s2\\d+\\s\\d+\\s(\\d+).*$'''
            p = re.compile(pattern)
            m = p.match(line)
            if m:
                time_h, time_m, time_s, ip, referrer, user_id, live_id, down_flow = m.groups()
                start_second = self.time_to_second(time_h, time_m, time_s)
                return 'hls', ip, user_id, live_id, start_second, '0', '0', down_flow, self.getdomain(referrer)
        return None

    def parse_rtmp_line_netcenter_live(self, line):
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

    def parse_rtmp_line_alicdn_live(self, line):
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

    def parse_rtmp_line_dnion_live(self, line):
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

    def parse_rtmp_line_tencent_live(self, line):
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

    def parse_hdl_line_netcenter_live(self, line):
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

    def parse_hdl_line_dnion_live(self, line):
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

    def parse_hdl_line_alicdn_live(self, line):
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


class LogDistinguish(object):
    """识别日志"""
    @classmethod
    def distinguish(cls, input_file_name):
        """
        识别日志
        :param input_file_name: 输入日志文件名称
        :return: date_str, cdn_code, type_code, protocol_list
        """
        res = cls.distinguish_netcenter(input_file_name)
        if res is not None:
            return res
        res = cls.distinguish_dnion(input_file_name)
        if res is not None:
            return res
        res = cls.distinguish_alicdn(input_file_name)
        if res is not None:
            return res
        res = cls.distinguish_tencent(input_file_name)
        if res is not None:
            return res
        return None

    @staticmethod
    def distinguish_netcenter(input_file_name):
        """
        识别网宿日志
        :param input_file_name:  example: 2017-12-06-2300-2330_rtmp-wsz.qukanvideo.com.cn.log.gz
        :return: date_str, cdn_code, type_code, protocol_list
        """
        pattern = '''^(\\d{4})-(\\d{2})-(\\d{2})-\\d{4}-\\d{4}_(.*?).cn.log$'''
        p = re.compile(pattern)
        m = p.match(input_file_name)
        if m:
            year, month, day, domain = m.groups()
            try:
                cdn_code, type_code, protocol = domain_protocol_dict[domain]
                return '{}-{}-{}'.format(year, month, day), cdn_code, type_code, [protocol]
            except (KeyError, ValueError):
                return None
        return None

    @staticmethod
    def distinguish_dnion(input_file_name):
        """
        识别帝联日志
        :param input_file_name:  example: hls-d.quklive.com_20180509_03_04.gz
        :return: date_str, cdn_code, type_code, protocol_list
        """
        pattern = '''^(.*?)_(\\d{4})(\\d{2})(\\d{2})_\\d{2}_\\d{2}$'''
        p = re.compile(pattern)
        m = p.match(input_file_name)
        if m:
            domain, year, month, day = m.groups()
            try:
                cdn_code, type_code, protocol = domain_protocol_dict[domain]
                return '{}-{}-{}'.format(year, month, day), cdn_code, type_code, [protocol]
            except (KeyError, ValueError):
                return None
        return None

    @staticmethod
    def distinguish_alicdn(input_file_name):
        """
        识别阿里日志
        :param input_file_name:  example: play-a.quklive.com_2017_12_07_1100_1200.gz | recordcdn-sz.qukanvideo.com_2017_12_06_1800_1900.gz
        :return: date_str, cdn_code, type_code, protocol_list
        """
        pattern = '''^(.*?)_(\\d{4})_(\\d{2})_(\\d{2})_\\d{4}_\\d{4}$'''
        p = re.compile(pattern)
        m = p.match(input_file_name)
        if m:
            domain, year, month, day = m.groups()
            try:
                cdn_code, type_code, protocol = domain_protocol_dict[domain]
                return '{}-{}-{}'.format(year, month, day), cdn_code, type_code, [protocol]
            except (KeyError, ValueError):
                return None
        return None

    @staticmethod
    def distinguish_tencent(input_file_name):
        """
        识别tencent日志
        :param input_file_name:  example: 2017120607_hangzhouqukan.cdn.log.gz
        :return: date_str, cdn_code, type_code, protocol_list
        """
        pattern = '''^(\\d{4})(\\d{2})(\\d{2})\\d{2}_hangzhouqukan.cdn.log$'''
        p = re.compile(pattern)
        m = p.match(input_file_name)
        print input_file_name
        if m:
            year, month, day = m.groups()
            try:
                return '{}-{}-{}'.format(year, month, day), 'tencent', 'live', ['rtmpdist', 'hls']
            except (KeyError, ValueError):
                return None
        return None


def init():
    """初始化"""
    global line_parser
    line_parser = LogLineParser()


def read_input(std_input):
    """生成器：读取输入"""
    for line in std_input:
        yield line


def flow_statistic_mapper():
    # 这两行必须放在一定会执行到的地方（即不能放在某个条件语句中），比如最前面，否则hadoop会认为没有读取stdin，因此直接报错
    data = read_input(sys.stdin)
    input_file_path = os.environ['mapreduce_map_input_file']
    # input_file_path = '/tmp/2018-05-09-0000-0030_rtmpdist-wsz.qukanvideo.com.cn.log.gz'
    input_file_name = input_file_path.split('/')[-1][:-3]  # 去掉后面的gz，便于本地命令行调试
    res = LogDistinguish.distinguish(input_file_name)
    if res is not None:
        date_str, cdn_code, type_code, protocol_list = res
        for line in data:
            for protocol in protocol_list:
                res = line_parser.parse(date_str, cdn_code, type_code, protocol, line.strip())
                if res is not None:
                    print res


# @click.command()
# @click.option('--cdn_code', default='netcenter', help='supported cdn code: netcenter|dnion|alicdn|tencent')
# @click.option('--protocol', default='rtmp', help='supported protocol: rtmp|hdl|hls')
# def run(cdn_code, protocol):
#     flow_statistic_mapper(cdn_code, protocol)


if __name__ == '__main__':
    init()
    flow_statistic_mapper()
