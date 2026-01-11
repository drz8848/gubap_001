def 更新任务已爬页数(conn, cursor, 股票代码, 总页数, 当前爬取页码):
    """
    更新stock_crawl_task_log表的crawl_page字段（已爬取页数）
    核心计算：已爬页数 = 总页数 - 当前爬取页码 + 1（因爬虫从后往前爬取：总页数→1）
    参数：
        conn: 数据库连接对象
        cursor: 数据库游标对象
        股票代码: 目标股票代码（如"000001"）
        总页数: 该股票股吧的总页数
        当前爬取页码: 本次爬取的页码（从总页数往1递减）
    返回：
        bool: True=更新成功，False=更新失败
    """
    if not (conn and cursor):
        print("❌ 数据库连接未建立，无法更新爬取页数")
        return False
    
    # 1. 计算已爬页数（避免负数，兜底为0）
    已爬页数 = max(0, 总页数 - 当前爬取页码 + 1)
    if 已爬页数 <= 0:
        print(f"⚠️ 无需更新爬取页数（股票：{股票代码}，当前页码：{当前爬取页码}）")
        return True
    
    # 2. 构造更新SQL（删除不存在的update_time字段，匹配原表结构）
    update_sql = """
    UPDATE stock_crawl_task_log
    SET crawl_page = %s
    WHERE stock_code = %s
    AND crawl_status = 'running'
    ORDER BY id DESC
    LIMIT 1;  -- 取最新的运行中任务，避免重复更新
    """
    
    # 3. 执行SQL并提交事务
    try:
        cursor.execute(update_sql, (已爬页数, 股票代码))
        conn.commit()
        if cursor.rowcount > 0:
            print(f"✅ 爬取页数更新成功（股票：{股票代码}，已爬：{已爬页数}/{总页数}页）")
            return True
        else:
            print(f"⚠️ 未找到可更新的任务（股票：{股票代码}，无运行中任务）")
            return False
    except Exception as e:
        conn.rollback()
        print(f"❌ 更新爬取页数失败：{str(e)} | 股票：{股票代码}")
        return False