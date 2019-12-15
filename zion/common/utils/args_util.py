#!/usr/bin/env python
# -*- coding:utf-8 -*-

"""
:Description: 参数处理
:Owner: zengzheng.zeal
:Create time: 2019-12-13
"""
from datetime import datetime, timedelta


def args_parse():
    """
    处理  name_xxx=value_xxx
    这种格式的参数 返回dict
    :return: dict
    """
    import sys
    para = dict([p.split('=', 1) for p in sys.argv[1:]])
    print(para)
    if 'execution_date' in para:
        date = (datetime.strptime(para['execution_date'][:19], "%Y-%m-%dT%H:%M:%S") + timedelta(hours=8)).strftime(
            "%Y%m%d")
        para["date"] = date
    return para
