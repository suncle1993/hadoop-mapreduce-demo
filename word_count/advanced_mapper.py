#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Created on 4/17/18 3:23 PM
@author: Chen Liang
@function: 更高级的Mapper，使用Python迭代器和生成器
"""

import sys


def read_input(std_input):
    for line in std_input:
        # 将line分割成单词
        yield line.split()


def main(separator='\t'):
    # 从标准输入STDIN输入
    data = read_input(sys.stdin)
    for words in data:
        # 将结果写到标准输出，此处的输出会作为reduce的输入
        for word in words:
            print('{}{}{}'.format(word, separator, 1))

if __name__ == "__main__":
    main()
