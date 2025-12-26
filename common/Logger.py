#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import logging
import os
from logging.handlers import RotatingFileHandler

def getLogger(log_file_path):
    """
    创建可滚动分割的日志对象
    :param log_file_path: 日志文件完整路径
    :return: logging.Logger对象
    """
    # 创建日志目录
    log_dir = os.path.dirname(log_file_path)
    if not os.path.exists(log_dir):
        os.makedirs(log_dir, exist_ok=True)

    # 日志格式配置
    log_formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(funcName)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # 创建logger对象
    logger = logging.getLogger(log_file_path)
    logger.setLevel(logging.INFO)
    logger.handlers.clear()  # 清除重复处理器

    # 文件处理器（按大小分割，最大100MB，保留5个备份）
    file_handler = RotatingFileHandler(
        log_file_path,
        maxBytes=1024 * 1024 * 100,
        backupCount=5,
        encoding='utf-8'
    )
    file_handler.setFormatter(log_formatter)

    # 控制台处理器（输出到终端）
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_formatter)

    # 添加处理器
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger
