#!/usr/bin/env python
# -*- coding:utf-8 -*-

"""
:Description: 一些配置
:Owner: zengzheng.zeal
:Create time: 2019-12-12
"""

env = 'rd'

__env_idx = {
    'rd': 0, 'dev': 1, 'pro': 2
}
from .hive_db import hive_db_conf


def set_env(e):
    global env
    env = e
    for k, v in hive_db_conf.items():
        globals()[k] = v[__env_idx[env]]


set_env(env)
