from 异步数据库连接 import 提交事务, 回滚事务

async def 更新任务已爬页数(conn, cursor, 股票代码, 总页数, 当前爬取页码):
    """异步更新已爬页数（总页数-当前页码+1）"""
    if not (conn and cursor):
        print("❌ 数据库连接未建立，无法更新进度")
        return False
    
    # 计算已爬页数
    已爬页数 = max(0, 总页数 - 当前爬取页码 + 1)
    if 已爬页数 <= 0:
        print(f"⚠️ 无需更新（股票：{股票代码}，当前页码：{当前爬取页码}）")
        return True
    
    # 更新SQL
    update_sql = """
    UPDATE stock_crawl_task_log
    SET crawl_page = %s
    WHERE stock_code = %s
    AND crawl_status = 'running'
    ORDER BY id DESC
    LIMIT 1;
    """
    
    # 异步执行
    try:
        await cursor.execute(update_sql, (已爬页数, 股票代码))
        await 提交事务(conn)
        if cursor.rowcount > 0:
            print(f"✅ 进度更新成功（股票：{股票代码}，已爬：{已爬页数}/{总页数}页）")
            return True
        else:
            print(f"⚠️ 未找到可更新任务（股票：{股票代码}）")
            return False
    except Exception as e:
        await 回滚事务(conn)
        print(f"❌ 更新进度失败：{str(e)} | 股票：{股票代码}")
        return False