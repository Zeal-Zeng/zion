#!/usr/bin/env python
# -*- coding:utf-8 -*-

"""
:Description: 记得写注释
:Owner: zengzheng.zeal
:Create time: 2019-11-01
"""
from zion.common.data_sync.mysql_to_hive import MysqlExtractor


def main(para):
    MysqlExtractor(para["db"], para["tb"]).extract_to_hive(
        date=para["date"] if para.get("inc") else None,
        partition_column=para.get("column"),
        per_partition_num=para.get("per_partition_num")
    )

