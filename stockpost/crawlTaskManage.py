#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import json
from common.Logger import getLogger

# 初始化日志
logger = getLogger(os.path.join(os.path.dirname(os.path.dirname(__file__)), "logs", "crawl_task.log"))

class CrawlTaskManage:
    """爬取任务管理类，实现断点续爬与缓存管理"""
    def __init__(self, cache_dir):
        self.cache_dir = cache_dir
        os.makedirs(self.cache_dir, exist_ok=True)
        self.task_cache_file = os.path.join(self.cache_dir, "crawl_task_cache.json")
        # 加载已爬取任务缓存
        self.crawled_task = self._load_cache()

    def _load_cache(self):
        """加载已爬取任务缓存"""
        if os.path.exists(self.task_cache_file):
            try:
                with open(self.task_cache_file, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception as e:
                logger.error(f"加载任务缓存失败：{e}")
                return {}
        return {}

    def _save_cache(self):
        """保存任务缓存到文件"""
        try:
            with open(self.task_cache_file, "w", encoding="utf-8") as f:
                json.dump(self.crawled_task, f, ensure_ascii=False, indent=2)
            logger.debug("任务缓存已保存")
        except Exception as e:
            logger.error(f"保存任务缓存失败：{e}")

    def is_crawled(self, stock_code, page):
        """判断该股票的该页码是否已爬取"""
        key = f"{stock_code}_{page}"
        return self.crawled_task.get(key, False)

    def mark_crawled(self, stock_code, page):
        """标记该股票的该页码为已爬取"""
        key = f"{stock_code}_{page}"
        self.crawled_task[key] = True
        self._save_cache()
        logger.debug(f"标记已爬取：{key}")

    def clear_cache(self, stock_code=None):
        """清除缓存（可选清除指定股票）"""
        if stock_code:
            for key in list(self.crawled_task.keys()):
                if key.startswith(f"{stock_code}_"):
                    del self.crawled_task[key]
            logger.info(f"清除股票 {stock_code} 的爬取缓存")
        else:
            self.crawled_task.clear()
            logger.info("清除所有爬取缓存")
        self._save_cache()

# 全局任务管理实例
task_manager = None

def init_task_manager(cache_dir):
    """初始化任务管理实例"""
    global task_manager
    task_manager = CrawlTaskManage(cache_dir)
    return task_manager

def get_task_manager():
    """获取全局任务管理实例"""
    return task_manager