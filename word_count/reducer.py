#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Created on 4/17/18 11:16 AM
@author: Chen Liang
@function: word count reducer
"""

import sys

current_word = None
current_count = 0
word = None

for line in sys.stdin:
    # 移除line收尾的空白字符
    line = line.strip()

    # 解析我们从mapper.py得到的输入
    word, count = line.split('\t', 1)

    # 将字符串count转换为int
    try:
        count = int(count)
    except ValueError:
        # 不是数字，不做处理，跳过
        continue

    # hadoop在将kv对传递给reduce之前会进行按照key进行排序，在这里也就是word
    if current_word == word:
        current_count += count
    else:
        if current_word is not None:
            # 将结果写入STDOUT
            print('{}\t{}'.format(current_word, current_count))
        current_count = count
        current_word = word

# 最后一个单词不要忘记输出
if current_word == word:
    print('{}\t{}'.format(current_word, current_count))
