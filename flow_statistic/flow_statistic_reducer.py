#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Created on 5/10/18 2:22 PM
@author: Chen Liang
@function: reducer
"""

from itertools import groupby
from operator import itemgetter
import sys
import requests
import json

ip_dict = {}


def is_internal(ip):
    """判断ip是否在国内"""
    if ip_dict.get(ip, -1) != -1:
        return ip_dict[ip]
    payload = {
        'ip': ip
    }
    try:
        r = requests.get(
            url="http://127.0.0.1/api/ip_query",
            params=payload
        )
        if r.status_code == 200:
            json_data = json.loads(r.text)
            if json_data['country_code'] == 'CN':
                ip_dict[ip] = True
                return True
            else:
                ip_dict[ip] = False
                return False
    except (KeyError, Exception):
        return False  # 出异常则为国外流量


def read_mapper_output(std_input, separator='\t'):
    """生成器：读取mapper的输出"""
    for line in std_input:
        yield line.rstrip().split(separator, 1)


def main(separator='\t'):
    data = read_mapper_output(sys.stdin, separator=separator)
    for live_id, group in groupby(data, itemgetter(0)):
        internal_flow = oversea_flow = 0
        try:
            for _, rest in group:
                _, ip, up_flow, down_flow = rest.split('\t')
                if is_internal(ip):
                    internal_flow += int(up_flow) + int(down_flow)
                else:
                    oversea_flow += int(up_flow) + int(down_flow)
            print('{1}{0}{2}{0}{3}{0}{4}'.format(
                separator, live_id, internal_flow + oversea_flow, internal_flow, oversea_flow
            ))
        except ValueError:
            pass


if __name__ == '__main__':
    main()
