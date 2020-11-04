# !/usr/bin/env python
# -*- coding: utf-8 -*-

import MySQLdb
from retrying import retry


class ConnConfig:
    """Redis和MySQL连接"""

    def __init__(self):
        # Remote Connection string
        self.mysql_host = 'your_mysql_host'
        self.mysql_port = 3306
        self.mysql_user = 'your_mysql_user'
        self.mysql_password = 'your_mysql_password'
        self.mysql_database = 'your_mysql_database_name'
        self.charset = 'utf8'

        # Local Connection string
        self.local_mysql_host = 'your_mysql_host'
        self.local_mysql_user = 'your_mysql_user'
        self.local_mysql_password = 'your_mysql_password'
        self.local_mysql_database = 'your_mysql_database_name'

    @retry(stop_max_attempt_number=10, wait_fixed=2)
    def Conn_MySQL(self):
        """连接MySQL数据库
        链接失败，重试10次，每次间隔2s"""
        # 打开数据库连接
        db = MySQLdb.connect(host=self.mysql_host, port=self.mysql_port, user=self.mysql_user,
                             passwd=self.mysql_password,
                             db=self.mysql_database, charset=self.charset)
        return db

    @retry(stop_max_attempt_number=10, wait_fixed=2)
    def Conn_Local_MySQL(self):
        """连接MySQL数据库
        链接失败，重试10次，每次间隔2s"""
        # 打开数据库连接
        db = MySQLdb.connect(host=self.local_mysql_host, port=self.mysql_port, user=self.local_mysql_user,
                             passwd=self.local_mysql_password,
                             db=self.local_mysql_database, charset=self.charset)

        return db
