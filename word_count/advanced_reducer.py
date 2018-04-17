#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Created on 4/17/18 3:23 PM
@author: Chen Liang
@function: 更高级的Reducer，使用Python迭代器和生成器
"""

from itertools import groupby
from operator import itemgetter
import sys


def read_mapper_output(std_input, separator='\t'):
    for line in std_input:
        yield line.rstrip().split(separator, 1)


def main(separator='\t'):
    # 从STDIN输入
    data = read_mapper_output(sys.stdin, separator=separator)
    # groupby通过word对多个word-count对进行分组，并创建一个返回连续键和它们的组的迭代器：
    #  - current_word - 包含单词的字符串（键）
    #  - group - 是一个迭代器，能产生所有的["current_word", "count"]项
    # itemgetter: 用于获取对象的哪些维的数据，itemgetter(0)表示获取第0维
    for current_word, group in groupby(data, itemgetter(0)):
        try:
            total_count = sum(int(count) for current_word, count in group)
            print('{}{}{}'.format(current_word, separator, total_count))
        except ValueError:
            pass

if __name__ == '__main__':
    main()
