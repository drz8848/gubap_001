#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import pymysql
import os
from common.Logger import getLogger

# 初始化日志
logger = getLogger(os.path.join(os.path.dirname(os.path.dirname(__file__)), "logs", "mysql_db.log"))

class MysqlDB:
    """MySQL数据库操作类，适配原代码的数据库调用逻辑"""
    _conn = None
    _cursor = None

    def __init__(self, host, port, user, password, db, charset='utf8mb4'):
        self.host = host
        self.port = int(port)
        self.user = user
        self.password = password
        self.db = db
        self.charset = charset

    def connect(self):
        """建立数据库连接"""
        try:
            self._conn = pymysql.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.db,
                charset=self.charset,
                cursorclass=pymysql.cursors.DictCursor
            )
            self._cursor = self._conn.cursor()
            logger.info("MySQL数据库连接成功")
            return True
        except pymysql.MySQLError as e:
            logger.error(f"MySQL连接失败：{e}")
            return False

    def close(self):
        """关闭数据库连接"""
        try:
            if self._cursor:
                self._cursor.close()
            if self._conn:
                self._conn.close()
            logger.info("MySQL数据库连接已关闭")
        except pymysql.MySQLError as e:
            logger.error(f"关闭MySQL连接失败：{e}")

    def execute_sql(self, sql, params=None):
        """
        执行单条SQL语句（增/删/改/查）
        :param sql: SQL语句
        :param params: SQL参数（元组/列表）
        :return: 成功返回True/查询结果，失败返回False
        """
        try:
            if not self._conn:
                if not self.connect():
                    return False

            self._cursor.execute(sql, params or ())
            self._conn.commit()
            logger.debug(f"SQL执行成功：{sql}，参数：{params}")
            return self._cursor.fetchall() if sql.strip().upper().startswith("SELECT") else True
        except pymysql.MySQLError as e:
            self._conn.rollback()
            logger.error(f"SQL执行失败：{sql}，参数：{params}，错误：{e}")
            return False

    def batch_execute_sql(self, sql, params_list):
        """
        批量执行SQL语句（适用于批量插入）
        :param sql: SQL语句模板
        :param params_list: 参数列表（列表嵌套元组）
        :return: 成功返回True，失败返回False
        """
        try:
            if not self._conn:
                if not self.connect():
                    return False

            self._cursor.executemany(sql, params_list)
            self._conn.commit()
            logger.info(f"批量SQL执行成功，共执行 {len(params_list)} 条记录")
            return True
        except pymysql.MySQLError as e:
            self._conn.rollback()
            logger.error(f"批量SQL执行失败：{sql}，错误：{e}")
            return False

# 全局数据库实例（从配置文件读取参数后初始化）
mysql_client = None

def init_mysql(config):
    """
    初始化MySQL全局实例
    :param config: 配置文件对象（ConfigParser）
    :return: 初始化后的mysql_client
    """
    global mysql_client
    try:
        mysql_client = MysqlDB(
            host=config.get("MYSQL", "HOST"),
            port=config.get("MYSQL", "PORT"),
            user=config.get("MYSQL", "USER"),
            password=config.get("MYSQL", "PASSWORD"),
            db=config.get("MYSQL", "DB_NAME")
        )
        # 尝试连接
        mysql_client.connect()
        # 创建股吧数据表格（若不存在）
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS guba_stock_post (
            id INT AUTO_INCREMENT PRIMARY KEY COMMENT '自增ID',
            stock_code VARCHAR(20) NOT NULL COMMENT '股票代码',
            post_title VARCHAR(500) NOT NULL COMMENT '帖子标题',
            author_name VARCHAR(100) DEFAULT '' COMMENT '作者名称',
            author_id VARCHAR(100) DEFAULT '' COMMENT '作者ID',
            author_url VARCHAR(500) DEFAULT '' COMMENT '作者主页链接',
            publish_time VARCHAR(50) DEFAULT '' COMMENT '发表时间',
            read_count VARCHAR(20) DEFAULT '0' COMMENT '阅读数',
            comment_count VARCHAR(20) DEFAULT '0' COMMENT '评论数',
            like_count VARCHAR(20) DEFAULT '0' COMMENT '点赞数',
            post_url VARCHAR(500) DEFAULT '' COMMENT '帖子链接',
            crawl_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '爬取时间',
            INDEX idx_stock_code (stock_code),
            INDEX idx_publish_time (publish_time)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='东方财富股吧帖子数据表';
        """
        mysql_client.execute_sql(create_table_sql)
        return mysql_client
    except Exception as e:
        logger.error(f"初始化MySQL实例失败：{e}")
        return None

def get_mysql_client():
    """获取全局MySQL实例"""
    return mysql_client