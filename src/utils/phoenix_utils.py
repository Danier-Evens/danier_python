#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time      : 2017/8/3 下午2:00
# @Author    : 魏晓辉[weixiaohui@souche.com]
# @CopyRight : DataTeam @ SouChe Inc
# @Desc      :


import phoenixdb

class PhoenixUtils():
    """
    > The Phoenix Query Server provides an alternative means for interaction with Phoenix and HBase.
    Soon this will enable access from environments other than the JVM.
    :param database_url: 数据库地址
    参考:http://python-phoenixdb.readthedocs.io/en/latest/
        http://phoenix.apache.org/server.html
    """

    def __init__(self, database_url):
        self.database_url = database_url
        self.conn = phoenixdb.connect(database_url, autocommit=True)

    def batch_insert(self, sql):
        pass

    def insert(self, sql):
        cursor = self.conn.cursor()
        cursor.execute(sql)