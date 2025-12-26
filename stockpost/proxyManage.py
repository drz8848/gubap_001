#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import requests
import os
from common.Logger import getLogger

# 初始化日志
logger = getLogger(os.path.join(os.path.dirname(os.path.dirname(__file__)), "logs", "proxy_manage.log"))

class ProxyManage:
    """代理管理类，支持获取代理与验证代理有效性"""
    def __init__(self, proxy_enable, proxy_pool_url):
        self.proxy_enable = proxy_enable
        self.proxy_pool_url = proxy_pool_url
        self.current_proxy = None

    def get_proxy(self):
        """从代理池获取一个可用代理"""
        if not self.proxy_enable:
            return None

        try:
            response = requests.get(self.proxy_pool_url, timeout=5)
            response.raise_for_status()
            proxy = response.text.strip()
            if proxy:
                self.current_proxy = {"http": f"http://{proxy}", "https": f"https://{proxy}"}
                logger.info(f"获取到代理：{proxy}")
                return self.current_proxy
            else:
                logger.warning("代理池返回空代理")
                return None
        except Exception as e:
            logger.error(f"获取代理失败：{e}")
            return None

    def validate_proxy(self, proxy):
        """验证代理是否有效"""
        if not proxy:
            return False

        try:
            test_url = "https://www.baidu.com"
            response = requests.get(test_url, proxies=proxy, timeout=5)
            return response.status_code == 200
        except Exception as e:
            logger.error(f"代理验证失败：{proxy}，错误：{e}")
            return False

# 全局代理管理实例
proxy_manager = None

def init_proxy_manager(config):
    """初始化代理管理实例"""
    global proxy_manager
    proxy_manager = ProxyManage(
        proxy_enable=config.getboolean("PROXY", "PROXY_ENABLE"),
        proxy_pool_url=config.get("PROXY", "PROXY_POOL_URL")
    )
    return proxy_manager

def get_proxy_manager():
    """获取全局代理管理实例"""
    return proxy_manager