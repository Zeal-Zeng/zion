#!/usr/bin/env python
# -*- coding:utf-8 -*-

"""
:Description: 记得写注释
:Owner: zengzheng.zeal
:Create time: 2019-12-13
"""
import os
import sys
path = os.path.abspath(__file__)
path = path[:path.rfind('zion')]

sys.path.insert(0, path)
print(path)
import zion


if __name__ == '__main__':

    para = zion.get_args()
    module = __import__(para["module"], fromlist="dumy")
    module.main(para)
