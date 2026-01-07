import datetime
from 连接数据库 import MYSQL_CONFIG

def 写入单篇帖子数据(conn, cursor, 帖子数据字典):
    """
    将单篇股吧帖子数据写入stock_guba_post表，避免重复存储（更新已有数据）
    核心逻辑：利用post_url唯一约束，INSERT ... ON DUPLICATE KEY UPDATE 实现「无则插入、有则更新」
    参数：
        conn: 数据库连接对象（已建立，不重复创建）
        cursor: 数据库游标对象（已建立，不重复创建）
        帖子数据字典: 包含帖子所有字段的字典，字段需与表结构对应
    返回：
        bool: True=操作成功，False=操作失败
    """
    if not (conn and cursor):
        print("❌ 数据库连接未建立，无法写入帖子数据")
        return False
    
    # 1. 构造插入/更新SQL语句（匹配stock_guba_post表结构）
    insert_update_sql = """
    INSERT INTO stock_guba_post (
        stock_code,
        post_title,
        post_url,
        post_read_count,
        post_comment_count,
        like_num,
        author_name,
        post_update_time,
        post_create_time,
        crawl_time,
        post_source
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
    ) ON DUPLICATE KEY UPDATE
        stock_code = VALUES(stock_code),
        post_title = VALUES(post_title),
        post_read_count = VALUES(post_read_count),
        post_comment_count = VALUES(post_comment_count),
        like_num = VALUES(like_num),
        author_name = VALUES(author_name),
        post_update_time = VALUES(post_update_time),
        post_create_time = VALUES(post_create_time),
        crawl_time = VALUES(crawl_time),
        post_source = VALUES(post_source)
    """
    
    # 2. 处理数据兜底（避免None值导致SQL报错，字段与SQL对应）
    兜底数据 = {
        "stock_code": "",
        "post_title": "",
        "post_url": "",
        "post_read_count": "0",
        "post_comment_count": "0",
        "like_num": -1,
        "author_name": "",
        "post_update_time": "",
        "post_create_time": None,
        "crawl_time": datetime.datetime.now(),
        "post_source": "guba.eastmoney.com"
    }
    
    # 合并用户传入数据与兜底数据（用户数据覆盖兜底数据）
    最终帖子数据 = {**兜底数据, **帖子数据字典}
    
    # 3. 构造SQL参数元组（与SQL语句字段顺序严格对应）
    sql_params = (
        最终帖子数据["stock_code"],
        最终帖子数据["post_title"],
        最终帖子数据["post_url"],
        最终帖子数据["post_read_count"],
        最终帖子数据["post_comment_count"],
        最终帖子数据["like_num"],
        最终帖子数据["author_name"],
        最终帖子数据["post_update_time"],
        最终帖子数据["post_create_time"],
        最终帖子数据["crawl_time"],
        最终帖子数据["post_source"]
    )
    
    # 4. 执行SQL并提交事务
    try:
        cursor.execute(insert_update_sql, sql_params)
        conn.commit()
        # 区分插入和更新（便于日志查看）
        if cursor.rowcount == 1:
            print(f"✅ 帖子插入成功：{最终帖子数据['post_url'][:50]}...")
        else:
            print(f"✅ 帖子已存在，更新成功：{最终帖子数据['post_url'][:50]}...")
        return True
    except Exception as e:
        conn.rollback()
        print(f"❌ 写入/更新帖子失败：{str(e)} | URL：{最终帖子数据['post_url'][:50]}...")
        return False