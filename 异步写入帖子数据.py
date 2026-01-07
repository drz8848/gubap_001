import datetime
from 异步数据库连接 import 提交事务, 回滚事务

async def 写入单篇帖子数据(conn, cursor, 帖子数据字典):
    """异步写入/更新帖子数据到数据库"""
    if not (conn and cursor):
        print("❌ 数据库连接未建立，无法写入数据")
        return False
    
    # 修复：使用别名替代VALUES()函数（解决MySQL弃用警告）
    insert_update_sql = """
    INSERT INTO stock_guba_post (
        stock_code, post_title, post_url, post_read_count, post_comment_count,
        like_num, author_name, author_id, author_url, post_update_time,
        post_create_time, post_content, crawl_time, post_source
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
    ) AS new_post  -- 给INSERT语句起别名new_post
    ON DUPLICATE KEY UPDATE
        stock_code = new_post.stock_code,
        post_title = new_post.post_title,
        post_read_count = new_post.post_read_count,
        post_comment_count = new_post.post_comment_count,
        like_num = new_post.like_num,
        author_name = new_post.author_name,
        author_id = new_post.author_id,
        author_url = new_post.author_url,
        post_update_time = new_post.post_update_time,
        post_create_time = new_post.post_create_time,
        post_content = new_post.post_content,
        crawl_time = new_post.crawl_time,
        post_source = new_post.post_source
    """
    
    # 兜底数据
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
    
    # 合并数据
    最终帖子数据 = {**兜底数据, **帖子数据字典}
    
    # 构造参数
    sql_params = (
        最终帖子数据["stock_code"],
        最终帖子数据["post_title"],
        最终帖子数据["post_url"],
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
        最终帖子数据["post_source"]
    )
    
    # 异步执行SQL
    try:
        await cursor.execute(insert_update_sql, sql_params)
        await 提交事务(conn)
        if cursor.rowcount == 1:
            print(f"✅ 帖子插入成功：{最终帖子数据['post_url'][:50]}...")
        else:
            print(f"✅ 帖子更新成功：{最终帖子数据['post_url'][:50]}...")
        return True
    except Exception as e:
        await 回滚事务(conn)
        print(f"❌ 写入帖子失败：{str(e)} | URL：{最终帖子数据['post_url'][:50]}...")
        return False