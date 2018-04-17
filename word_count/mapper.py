#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Created on 4/17/18 11:16 AM
@author: Chen Liang
@function:  word count mapper
"""

import sys

# 从标准输入STDIN输入
for line in sys.stdin:
    # 移除line收尾的空白字符
    line = line.strip()
    # 将line分割为单词
    words = line.split()
    # 遍历
    for word in words:
        # 将结果写到标准输出STDOUT
        # 此处的输出会作为Reduce代码的输入
        print('{}\t{}'.format(word, 1))
