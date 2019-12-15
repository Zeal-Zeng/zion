#!/usr/bin/env python
# -*- coding:utf-8 -*-

"""
:Description: zion 数据仓库框架
root
|-- common 数据仓库核心代码
|-- base_data 业务脚本 核心基础数据 层
|-- ods 业务脚本 ODS 层
|-- bussiness  业务脚本 ADS 层

:Owner: zengzheng.zeal
:Create time: 2019-11-25
"""
from pyspark.sql import SparkSession
from .common.conf.hive_db import hive_db_conf
from .common.utils.args_util import args_parse
from zion.common.data_sync.mysql_to_hive import MysqlExtractor


env = 'rd'


def set_env(e):
    """
    :param e: 'rd','dev','pro' 任选一个
    :return:
    """
    global env
    env = e
    for k, v in hive_db_conf.items():
        globals()[k] = v[env]


def get_args():
    para = args_parse()
    set_env(para['env'])
    return para


def spark():
    return SparkSession.builder.enableHiveSupport().getOrCreate()


set_env(env)
