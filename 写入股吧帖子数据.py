import datetime
import sqlite3
from 连接数据库 import DB_PATH

def 写入单篇帖子数据(conn, cursor, 帖子数据字典):
    """
    将单篇股吧帖子数据写入stock_guba_post表，包含帖子正文post_content
    """
    if not (conn and cursor):
        print("❌ 数据库连接未建立，无法写入帖子数据")
        return False

    # 1. 检查是否已存在（通过post_url）
    check_sql = "SELECT id FROM stock_guba_post WHERE post_url = ?"
    cursor.execute(check_sql, (帖子数据字典.get("post_url", ""),))
    existing = cursor.fetchone()

    # 2. 处理数据兜底
    兜底数据 = {
        "stock_code": "",
        "post_title": "",
        "post_url": "",
        "post_read_count": "0",
        "post_comment_count": "0",
        "like_num": -1,
        "author_name": "",
        "author_id": "",
        "author_url": "",
        "post_update_time": "",
        "post_create_time": None,
        "post_content": "",
        "crawl_time": datetime.datetime.now(),
        "post_source": "guba.eastmoney.com"
    }

    # 合并用户传入数据与兜底数据
    最终帖子数据 = {**兜底数据, **帖子数据字典}

    try:
        if existing:
            # 3. 如果已存在，执行更新
            update_sql = """
            UPDATE stock_guba_post SET
                stock_code = ?,
                post_title = ?,
                post_read_count = ?,
                post_comment_count = ?,
                like_num = ?,
                author_name = ?,
                author_id = ?,
                author_url = ?,
                post_update_time = ?,
                post_create_time = ?,
                post_content = ?,
                crawl_time = ?,
                post_source = ?
            WHERE post_url = ?
            """
            update_params = (
                最终帖子数据["stock_code"],
                最终帖子数据["post_title"],
                最终帖子数据["post_read_count"],
                最终帖子数据["post_comment_count"],
                最终帖子数据["like_num"],
                最终帖子数据["author_name"],
                最终帖子数据["author_id"],
                最终帖子数据["author_url"],
                最终帖子数据["post_update_time"],
                最终帖子数据["post_create_time"],
                最终帖子数据["post_content"],
                最终帖子数据["crawl_time"],
                最终帖子数据["post_source"],
                最终帖子数据["post_url"]
            )
            cursor.execute(update_sql, update_params)
            print(f"✅ 帖子已存在，更新成功：{最终帖子数据['post_url'][:50]}...")
        else:
            # 4. 如果不存在，执行插入
            insert_sql = """
            INSERT INTO stock_guba_post (
                stock_code, post_title, post_url, post_read_count, post_comment_count,
                like_num, author_name, author_id, author_url, post_update_time,
                post_create_time, post_content, crawl_time, post_source
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            insert_params = (
                最终帖子数据["stock_code"], 最终帖子数据["post_title"], 最终帖子数据["post_url"],
                最终帖子数据["post_read_count"], 最终帖子数据["post_comment_count"],
                最终帖子数据["like_num"], 最终帖子数据["author_name"], 最终帖子数据["author_id"],
                最终帖子数据["author_url"], 最终帖子数据["post_update_time"],
                最终帖子数据["post_create_time"], 最终帖子数据["post_content"],
                最终帖子数据["crawl_time"], 最终帖子数据["post_source"]
            )
            cursor.execute(insert_sql, insert_params)
            print(f"✅ 帖子插入成功：{最终帖子数据['post_url'][:50]}...")

        conn.commit()
        return True
    except Exception as e:
        conn.rollback()
        print(f"❌ 写入/更新帖子失败：{str(e)} | URL：{最终帖子数据['post_url'][:50]}...")
        return False