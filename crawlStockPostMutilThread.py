#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import time
import datetime
import threading
import queue
from requests_html import HTMLSession
from bs4 import BeautifulSoup
import requests

# 导入自定义模块
from common.Logger import getLogger
from common.Config import getconfig
from mysql.mysql_db import init_mysql, get_mysql_client
from stockpost.crawlTaskManage import init_task_manager, get_task_manager
from stockpost.proxyManage import init_proxy_manager, get_proxy_manager

# 全局变量
CONFIG_PATH = os.path.join(os.path.dirname(__file__), "stockpost", "crawl.conf")
config = None
logger = None
mysql_client = None
task_manager = None
proxy_manager = None
crawl_queue = queue.Queue()
result_queue = queue.Queue()
current_year = datetime.datetime.now().year

def init_env():
    """初始化爬虫环境（配置、日志、数据库、任务管理、代理）"""
    global config, logger, mysql_client, task_manager, proxy_manager

    # 读取配置
    config = getconfig(CONFIG_PATH)
    logger = getLogger(os.path.join(config.get("BASE", "LOG_DIR"), "crawl_main.log"))

    # 初始化任务管理
    task_manager = init_task_manager(config.get("BASE", "CACHE_DIR"))

    # 初始化代理管理
    proxy_manager = init_proxy_manager(config)

    # 初始化数据库
    mysql_client = init_mysql(config)

    logger.info("爬虫环境初始化完成")

def build_url(stock_code, page):
    """构造股吧列表URL"""
    base_url = "https://guba.eastmoney.com/list,{stock_code}_{page}.html"
    return base_url.format(stock_code=stock_code, page=page)

def crawl_worker():
    """爬取工作线程（负责从队列获取任务，爬取页面）"""
    session = HTMLSession()
    session.headers = {
        "User-Agent": config.get("REQUEST", "USER_AGENT"),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "zh-CN,zh;q=0.9",
        "Connection": "keep-alive"
    }

    while True:
        try:
            task = crawl_queue.get(timeout=10)
            stock_code, page = task
            task_key = f"{stock_code}_{page}"

            # 跳过已爬取的任务
            if task_manager.is_crawled(stock_code, page):
                logger.info(f"跳过已爬取任务：{task_key}")
                crawl_queue.task_done()
                continue

            # 构造URL
            url = build_url(stock_code, page)
            logger.info(f"开始爬取：{task_key}，URL：{url}")

            # 获取代理
            proxy = proxy_manager.get_proxy() if proxy_manager else None

            # 爬取页面
            try:
                response = session.get(
                    url,
                    proxies=proxy,
                    timeout=int(config.get("REQUEST", "TIMEOUT"))
                )
                response.raise_for_status()
                response.encoding = "utf-8"
                time.sleep(float(config.get("BASE", "REQUEST_DELAY")))

                # 提交结果到队列
                result_queue.put((stock_code, page, url, response.text))
                logger.info(f"爬取成功：{task_key}")

            except Exception as e:
                logger.error(f"爬取失败：{task_key}，URL：{url}，错误：{e}")
                result_queue.put((stock_code, page, url, ""))

            finally:
                crawl_queue.task_done()

        except queue.Empty:
            logger.info("爬取队列已空，爬取线程退出")
            break
        except Exception as e:
            logger.error(f"爬取线程异常：{e}")
            continue

def parse_worker():
    """解析工作线程（负责解析爬取结果，提取数据）"""
    while True:
        try:
            result = result_queue.get(timeout=10)
            stock_code, page, url, html = result
            task_key = f"{stock_code}_{page}"

            if not html:
                logger.warning(f"无有效HTML，跳过解析：{task_key}")
                result_queue.task_done()
                continue

            # 解析页面
            try:
                soup = BeautifulSoup(html, "lxml")
                post_list = []

                # 定位帖子列表
                post_items = soup.find_all("div", class_="articleh normal_post")
                if not post_items:
                    post_items = soup.find_all("tr", class_="listitem")

                for item in post_items:
                    try:
                        # 阅读数
                        read_elem = item.find("span", class_="l1 a1") or item.find("div", class_="read")
                        read_count = read_elem.get_text().strip() if read_elem else "0"

                        # 评论数
                        comment_elem = item.find("span", class_="l2 a2") or item.find("div", class_="reply")
                        comment_count = comment_elem.get_text().strip() if comment_elem else "0"

                        # 标题 & 帖子链接
                        title_elem = item.find("span", class_="l3 a3") or item.find("div", class_="title")
                        title_a = title_elem.find("a") if title_elem else None
                        post_title = title_a.get("title", "").strip() if title_a else "无标题"
                        if not post_title and title_a:
                            post_title = title_a.get_text().strip()
                        post_url = title_a.get("href", "") if title_a else ""
                        if post_url and not post_url.startswith("http"):
                            post_url = "https://guba.eastmoney.com" + post_url

                        # 作者信息
                        author_elem = item.find("span", class_="l4 a4") or item.find("div", class_="author")
                        author_a = author_elem.find("a") if author_elem else None
                        author_id = author_a.get("data-popper", "") if (author_a and "data-popper" in author_a.attrs) else ""
                        author_name = author_a.get_text().strip() if author_a else "匿名"
                        author_url = author_a.get("href", "") if author_a else ""
                        if author_url and not author_url.startswith("http"):
                            author_url = "https://guba.eastmoney.com" + author_url

                        # 发表时间
                        time_elem = item.find("span", class_="l5 a5") or item.find("div", class_="update")
                        publish_time_str = time_elem.get_text().strip() if time_elem else ""
                        if publish_time_str and len(publish_time_str.split(" ")) == 2:
                            publish_time = f"{current_year}-{publish_time_str}:00"
                        else:
                            publish_time = publish_time_str

                        # 点赞数（列表页无，标记为需爬详情页）
                        like_count = "需爬详情页"

                        # 组装数据
                        post_data = (
                            stock_code,
                            post_title,
                            author_name,
                            author_id,
                            author_url,
                            publish_time,
                            read_count,
                            comment_count,
                            like_count,
                            post_url
                        )
                        post_list.append(post_data)

                    except Exception as e:
                        logger.error(f"解析单条帖子失败：{e}")
                        continue

                # 标记任务为已爬取
                task_manager.mark_crawled(stock_code, page)

                # 批量插入数据库
                if post_list and mysql_client:
                    insert_sql = """
                    INSERT INTO guba_stock_post (
                        stock_code, post_title, author_name, author_id, author_url,
                        publish_time, read_count, comment_count, like_count, post_url
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    mysql_client.batch_execute_sql(insert_sql, post_list)
                    logger.info(f"解析完成：{task_key}，提取 {len(post_list)} 条数据，已插入数据库")
                else:
                    logger.warning(f"解析完成：{task_key}，无有效数据或数据库未连接")

            except Exception as e:
                logger.error(f"解析页面失败：{task_key}，URL：{url}，错误：{e}")

            finally:
                result_queue.task_done()

        except queue.Empty:
            logger.info("结果队列已空，解析线程退出")
            break
        except Exception as e:
            logger.error(f"解析线程异常：{e}")
            continue

def init_crawl_queue():
    """初始化爬取队列（添加股票+页码任务）"""
    stock_codes = config.get("BASE", "STOCK_CODES").split(",")
    max_page = int(config.get("BASE", "MAX_PAGE"))

    for stock_code in stock_codes:
        stock_code = stock_code.strip()
        if not stock_code:
            continue
        for page in range(1, max_page + 1):
            crawl_queue.put((stock_code, page))

    logger.info(f"爬取队列初始化完成，共 {crawl_queue.qsize()} 个任务")

def start_threads():
    """启动爬取线程和解析线程"""
    thread_num = int(config.get("BASE", "THREAD_NUM"))

    # 启动爬取线程
    crawl_threads = []
    for i in range(thread_num):
        t = threading.Thread(target=crawl_worker, name=f"CrawlThread-{i+1}")
        t.daemon = True
        t.start()
        crawl_threads.append(t)
        logger.info(f"启动爬取线程：{t.name}")

    # 启动解析线程（解析线程数=爬取线程数/2，避免解析积压）
    parse_thread_num = max(1, thread_num // 2)
    parse_threads = []
    for i in range(parse_thread_num):
        t = threading.Thread(target=parse_worker, name=f"ParseThread-{i+1}")
        t.daemon = True
        t.start()
        parse_threads.append(t)
        logger.info(f"启动解析线程：{t.name}")

    # 等待爬取队列完成
    crawl_queue.join()
    logger.info("所有爬取任务已完成")

    # 等待结果队列完成
    result_queue.join()
    logger.info("所有解析任务已完成")

    # 等待线程退出
    for t in crawl_threads:
        t.join(timeout=5)
    for t in parse_threads:
        t.join(timeout=5)

def main():
    """爬虫主函数"""
    try:
        # 初始化环境
        init_env()

        # 初始化爬取队列
        init_crawl_queue()

        # 启动线程
        start_threads()

        logger.info("爬虫任务全部完成")
    except Exception as e:
        if logger:
            logger.error(f"爬虫主流程异常：{e}", exc_info=True)
        else:
            print(f"爬虫主流程异常：{e}")
    finally:
        # 关闭数据库连接
        if mysql_client:
            mysql_client.close()
        logger.info("爬虫资源已释放")

if __name__ == "__main__":
    main()