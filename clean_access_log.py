#!/usr/bin/env python
# -*- coding: UTF-8 -*-

from contextlib import closing
import MySQLdb

from config import conn_config, LoggingConfig


class CleanAccessLogMain:
    """
    清理SMS Receive 用户访问历史记录
    """

    def __init__(self):
        init_config = conn_config.ConnConfig()
        self._local_mysql = init_config.Conn_Local_MySQL()
        self._remote_mysql = init_config.Conn_MySQL()
        # restore log
        logger_name = 'clean_access_log'
        self._logger = LoggingConfig.init_logging(logger_name)
        self._temp_list_id = []

    def read_remote_access_log(self):
        init_config = conn_config.ConnConfig()
        with closing(init_config.Conn_MySQL()) as mysql_conn:
            with closing(mysql_conn.cursor()) as cur:
                sql = "SELECT * FROM SMS_Receive_Production.access_log JOIN (SELECT id FROM SMS_Receive_Production.access_log LIMIT 100000 OFFSET 0) AS t1 ON (t1.id=access_log.id);"
                # 执行SQL语句
                try:
                    cur.execute(sql)
                    # 获取所有记录列表
                    results = cur.rowcount
                except (MySQLdb.Error, MySQLdb.Warning) as e:
                    self._logger.error(f'执行 read_remote_access_log 函数时，SQL命令查询的时候出现错误，具体错误内容： {e}')
                    return False
                if results == 0:
                    # 数据是0，说明2018年数据已经完全删除,结束
                    print('数据已经清理完成')
                    return False
                else:
                    # 查询结果插入sms_content_info和sms_content表
                    get_all = cur.fetchall()
                    temp_list = []
                    for item in get_all:
                        id = item[0]
                        time = item[1]
                        ip = item[2]
                        url = item[3]
                        user_agent = item[4]
                        referer = item[5]
                        temp_list.append([time, ip, url, user_agent, referer])
                        self._temp_list_id.append(id)
                    return temp_list

    def write_local_access_log(self):
        # 将软成数据写入到本地数据库
        init_config = conn_config.ConnConfig()
        with closing(init_config.Conn_Local_MySQL()) as mysql_conn:
            with closing(mysql_conn.cursor()) as cur:
                sql = "INSERT INTO access_log(TIME, IP, URL, USER_AGENT, REFERER) VALUES (%s,%s,%s,%s,%s);"
                data_list = self.read_remote_access_log()
                try:
                    cur.executemany(sql, data_list)
                except (MySQLdb.Error, MySQLdb.Warning) as e:
                    self._logger.error(f'插入Access_Log出现错误，具体错误内容： {e}')
                mysql_conn.commit()
                # 数据提交之后清历史记录
            self.delete_remote_access_log()

    def delete_remote_access_log(self):
        init_config = conn_config.ConnConfig()
        with closing(init_config.Conn_MySQL()) as mysql_conn:
            with closing(mysql_conn.cursor()) as cur:
                if self._temp_list_id is not None:
                    for item in self._temp_list_id:
                        sql = f"DELETE FROM access_log WHERE id='{item}';"
                        try:
                            # 删除数据
                            cur.execute(sql)
                        except (MySQLdb.Error, MySQLdb.Warning) as e:
                            self._logger.error(f'删除Access_Log出现错误，具体错误内容： {e}')
                        mysql_conn.commit()
                    # 删除完成之后，清空列表
                    self._temp_list_id.clear()

    def main(self):
        while True:
            self.write_local_access_log()


if __name__ == '__main__':
    CleanAccessLogMain().main()
