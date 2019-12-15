#!/usr/bin/env python
# -*- coding:utf-8 -*-

"""
:Description: 通过spark jdbc从mysql导数据到到hive 注意内存给够 不然就多分区
:Owner: zengzheng.zeal
:Create time: 2019-11-04
"""

from pyspark.sql.functions import lit
from ..conf.mysql_conf import get_host_and_prop
import zion


class MysqlExtractor(object):
    """
    从MySQL提取数据到hive 依赖 conf 中的 mysql_conf
    """

    def __init__(self, db, tb):
        self.mysql_tb = tb
        self.mysql_db = db
        self.mysql_host, self.mysql_prop = get_host_and_prop(db)

    def _get_partition_upper_lower(self, host, user, password, db, tb, column):
        """
        获取分区字段的最小和最大值 方便做分区抽取
        :param host:
        :param user:
        :param password:
        :param db:
        :param tb:
        :param column:
        :return:
        """
        import pymysql
        con = pymysql.connect(host, user, password, db)
        with con:
            cur = con.cursor()
            cur.execute("select max(`{column}`),min(`{column}`) from `{db}`.`{tb}`".format(column=column, db=db, tb=tb))
            mm, mn = cur.fetchone()
        return mm + 1, mn

    def gen_df(self, partition_column=None, per_partition_num=4000000):
        """
        生成df
        :param partition_column: 用于分区的列名
        :param per_partition_num:  每个分区多少条数据
        :return:
        """
        lowerBound = None
        upperBound = None
        numPartitions = None
        host = self.mysql_host
        properties = {"driver": "com.mysql.jdbc.Driver"}
        properties.update(self.mysql_prop)
        url = "jdbc:mysql://" + host + "?serverTimezone=Asia/Shanghai"

        if partition_column:
            upperBound, lowerBound = self._get_partition_upper_lower(host, properties['user'], properties['password'],
                                                                     self.mysql_db, self.mysql_tb, partition_column)
            numPartitions = (upperBound - lowerBound) / per_partition_num

        df = zion.spark().read.jdbc(url, "`%s`.`%s`" % (self.mysql_db, self.mysql_tb), properties=properties,
                                    column=partition_column,
                                    lowerBound=lowerBound,
                                    upperBound=upperBound, numPartitions=numPartitions)
        return df

    def extract_to_hive(self, date=None, partition_column=None, per_partition_num=None,
                        hive_tb_name_fun=lambda tb: 'ods_' + tb):
        """
        mysql 到 hive
        :param per_partition_num:
        :param date: 用于date分区
        :param hive_tb_name_fun: 表名生成函数 会传入mysql tb做参数 默认加'ods_'前缀
        :param partition_column: 用于分区抽取数据的列
        :return:
        """
        if partition_column and not per_partition_num:
            per_partition_num = 4000000

        df = self.gen_df(partition_column, per_partition_num)

        df.registerTempTable("df")
        hive_db_tb = zion.ODS + "." + hive_tb_name_fun(self.mysql_tb)
        try:
            columns = zion.spark().table(hive_db_tb).drop("date").columns
            zion.spark().sql("insert overwrite table %s select %s from df " % (
                hive_db_tb + (" partition(date='%s') " % date if date else ''), ",".join(columns)))
        except Exception as e:
            if hive_db_tb in [zion.ODS + "." + i.name for i in zion.spark().catalog.listTables(dbName=zion.ODS)]:
                raise e
            else:
                if date:
                    df.withColumn("date", lit(date)).write.saveAsTable(hive_db_tb, partitionBy="date")
                else:
                    df.write.saveAsTable(hive_db_tb, mode='overwrite')
