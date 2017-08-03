#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time      : 2017/8/3 下午1:59
# @Author    : 魏晓辉[weixiaohui@souche.com]
# @CopyRight : DataTeam @ SouChe Inc
# @Desc      :


from DBUtils.PooledDB import PooledDB
import MySQLdb
import traceback

class MysqlUtils():
    def __init__(self, host, database, username, password):

        """
        mysql工具类
        :param host: 数据库地址
        :param username: 用户
        :param password: 密码
        :param database: 数据库
        """
        try:
            self.host = host
            self.database = database
            self.username = username
            self.password = password

            pool = PooledDB(MySQLdb, 10,
                            maxcached=20,
                            maxconnections=2,
                            blocking=True,
                            host=self.host,
                            user=self.username,
                            passwd=self.password,
                            db=self.database,
                            port=3306,
                            charset='utf8',
                            use_unicode=True)  # 5为连接池里的最少连接数
            self.pool = pool
        except Exception as e:
            print traceback.format_exc()
        finally:
            pass

    def fetchall(self, sql):
        conn = self.pool.connection()
        cursor = conn.cursor()
        try:
            cursor.execute(sql)
            return cursor.fetchall()
        except Exception as e:
            print traceback.format_exc()
        finally:
            cursor.close()
            conn.close()

    def fetchone(self, sql):
        conn = self.pool.connection()
        cursor = conn.cursor()
        try:
            cursor.execute(sql)
            return cursor.fetchone()
        except Exception as e:
            print traceback.format_exc()
        finally:
            cursor.close()
            conn.close()

    def close_conn(self):
        """
        关闭数据库

        """
        self.pool.close()
