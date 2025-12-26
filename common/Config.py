#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import configparser
import os

def getconfig(config_file_path):
    """
    读取ini格式配置文件
    :param config_file_path: 配置文件路径
    :return: configparser.ConfigParser对象
    """
    if not os.path.exists(config_file_path):
        raise FileNotFoundError(f"配置文件不存在：{config_file_path}")

    config = configparser.ConfigParser()
    config.optionxform = str  # 保留配置项大小写
    config.read(config_file_path, encoding='utf-8')
    return config