#!/usr/bin/env python
# -*- coding:utf-8 -*-

"""
:Description: 拉链表核心逻辑
:Owner: zengzheng.zeal
:Create time: 2019-09-18
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce, lit, concat_ws, expr, col
from datetime import datetime, timedelta

import zion


def gen_zipper_df(df_zipper, df_status, primary_cols, status_cols, date):
    """
    计算逻辑：
    zipper已有的left join status发现有有变更就改掉过期时间
    :param date: 前一天的日期 比如 20191213
    :param df_zipper: 要有created_at，start_at，expired_at 放在末尾 其他的状态字段在前面
    :param df_status: 要有created_at
    :param primary_cols: join的主键 list
    :type primary_cols:list
    :param status_cols:  判断状态是否变更的字段
    :return: df
    """

    is_change = (concat_ws('_*_', *[coalesce(df_status[c], lit('')) for c in status_cols])
                 != concat_ws('_*_', *[coalesce(df_zipper[c], lit('')) for c in status_cols]))
    # join上有变更的 和 没join上的(也就是记录消失的) is_change都会是ture 只有join上且没变更的会是false

    res_old_change = (df_zipper.join(df_status, primary_cols, "left")
                      .withColumn("_is_change", is_change)
                      .select(df_zipper["*"], "_is_change")
                      .withColumn("expired_at",
                                  expr("if(_is_change and expired_at is null,'%s 23:59:59.9',expired_at)" % (
                                      datetime.strptime(date, "%Y%m%d").strftime("%Y-%m-%d"))))
                      .filter("expired_at is not null")
                      .drop("_is_change")
                      )
    res_new_change = (
        df_status.join(df_zipper.filter("expired_at is null"), primary_cols, "left")
            .withColumn("_is_change", is_change)
            .select(df_status["*"], "_is_change",df_zipper['start_at'].alias("old_start"))
            .withColumn("start_at",
                        expr("if(_is_change,'%s',old_start)"%((datetime.strptime(date, "%Y%m%d") + timedelta(1)).strftime("%Y-%m-%d") + " 00:00:00.0"))
                        )
            .withColumn("expired_at", lit(None)).drop("old_start").drop("_is_change")
    )
    return res_old_change.unionAll(res_new_change)


class ZipperTable(object):
    def __init__(self, zipper_table, status_table, primary_cols, status_cols, date):
        """
        :param date: 前一天的日期 比如 20191213
        :param zipper_table: 要有created_at，start_at，expired_at 放在末尾 其他的状态字段在前面 需要
        :param status_table: 要有created_at
        :param primary_cols: join的主键 list
        :type primary_cols:list
        :param status_cols:  判断状态是否变更的字段
        """
        self.date = date
        self.status_cols = status_cols
        self.primary_cols = primary_cols
        self.zipper_table = zipper_table
        self.status_table = status_table

    def gen_df(self):
        return gen_zipper_df(
            zion.spark().table(self.zipper_table),
            zion.spark().table(self.status_table),
            self.primary_cols,
            self.status_cols,
            self.date
        )

    def gen_zipper_to_hive(self):
        self.gen_df().write.saveAsTable(self.zipper_table, mode='overwrite')

    def create_init_zipper_table(self):
        zion.spark().table(self.status_table).withColumn(
            "start_at", col("created_at")
        ).withColumn(
            "expired_at", lit(None).cast("string")
        ).write.saveAsTable(self.zipper_table, format="orc")

