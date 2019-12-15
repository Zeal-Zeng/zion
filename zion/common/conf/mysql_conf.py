#!/usr/bin/env python
# -*- coding:utf-8 -*-

"""
:Description: 阿里云上的mysql的配置信息
:Owner: zengzheng.zeal
:Create time: 2019-11-28
"""
import zion


def _k_listv_2_v_k(m):
    res = {}
    for k, v in m.items():
        for i in v:
            res[i] = k
    return res


_mysql_instance = {
    'dev': {

    },
    'pro': {

    },
    'rd': {
        '127.0.0.1:3306': {'user': 'u', 'password': '123456'},

    }
}

_db2instance = {
    'pro': _k_listv_2_v_k({
        '127.0.0.1:3306': ['test', 'test2']
    }),
    'dev': _k_listv_2_v_k({
        '127.0.0.1:3307': ['test', 'test2']
    }),
    'rd': _k_listv_2_v_k({
        '127.0.0.1:3308': ['test', 'test2'],
    }),
}


def get_host_and_prop(db):
    """
    通库名获取 host 和 相关连接信息
    :param db: 库名
    :return: host, {"user": ,"password":}
    """
    host = _db2instance[zion.env][db]
    return host, _mysql_instance[zion.env][host]
