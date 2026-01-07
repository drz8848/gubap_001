import aiomysql
import pandas as pd
import datetime
from 异步数据库连接 import MYSQL_CONFIG, 提交事务, 回滚事务

async def 初始化并读取爬取任务列表(conn, cursor, 从股票基础表补充任务: bool = True) -> pd.DataFrame:
    """异步初始化任务表，补充缺失任务，返回任务DataFrame
    【核心修正】移除所有整数显示宽度，符合MySQL 8.0+语法规范
    """
    # 完全符合MySQL 8.0+规范的建表语句
    创建任务表SQL = """
    CREATE TABLE IF NOT EXISTS stock_crawl_task_log (
        id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键',
        task_name VARCHAR(100) NOT NULL COMMENT '任务名称',
        stock_code VARCHAR(20) NOT NULL COMMENT '股票代码',
        guba_list_url VARCHAR(255) NOT NULL COMMENT '股吧链接',
        crawl_page INT UNSIGNED DEFAULT 0 COMMENT '已爬页数（非负）',
        last_crawl_post_time DATETIME DEFAULT NULL COMMENT '最后爬取时间',
        crawl_status VARCHAR(20) DEFAULT 'running' COMMENT '任务状态',
        error_msg TEXT DEFAULT NULL COMMENT '错误信息',
        crawl_start_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '开始时间',
        crawl_complete_time DATETIME DEFAULT NULL COMMENT '完成时间',
        PRIMARY KEY (id),
        KEY idx_stock_code_task (stock_code, task_name),
        KEY idx_crawl_status (crawl_status)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """
    
    任务数据框 = pd.DataFrame()
    
    try:
        if not (conn and cursor):
            print("❌ 数据库连接为空，无法处理任务表")
            return 任务数据框
        
        print("✅ 异步连接数据库成功，开始处理任务表")
        
        # 执行符合规范的建表语句
        await cursor.execute(创建任务表SQL)
        await 提交事务(conn)
        print("✅ 任务表创建/验证完成（语法符合MySQL 8.0+规范）")
        
        # 剩余逻辑保持不变（补充任务、读取任务）
        # -------- 以下代码和之前修复后的版本一致 --------
        if 从股票基础表补充任务:
            # 异步查询已有股票
            查询已有股票SQL = "SELECT DISTINCT stock_code FROM stock_crawl_task_log;"
            await cursor.execute(查询已有股票SQL)
            已有股票代码 = [row[0] for row in await cursor.fetchall()]
            
            # 异步查询待补充股票
            if 已有股票代码:
                查询股票SQL = """
                SELECT stock_code, guba_list_url FROM stock_bar_base 
                WHERE crawl_status = 'no_crawl' 
                AND stock_code NOT IN %s;
                """
                await cursor.execute(查询股票SQL, (tuple(已有股票代码),))
            else:
                查询股票SQL = """
                SELECT stock_code, guba_list_url FROM stock_bar_base 
                WHERE crawl_status = 'no_crawl';
                """
                await cursor.execute(查询股票SQL)
            
            股票数据 = await cursor.fetchall()
            
            if 股票数据:
                # 异步插入任务
                当前日期 = datetime.datetime.now().strftime("%Y%m%d")
                插入任务SQL = """
                INSERT INTO stock_crawl_task_log (
                    task_name, stock_code, guba_list_url, crawl_status
                ) VALUES (%s, %s, %s, %s);
                """
                
                任务计数 = 0
                for 股票代码, 股吧列表链接 in 股票数据:
                    任务名称 = f"{当前日期}_guba_crawl_{股票代码}"
                    await cursor.execute(插入任务SQL, (任务名称, 股票代码, 股吧列表链接, "running"))
                    任务计数 += 1
                
                await 提交事务(conn)
                print(f"✅ 成功补充{任务计数}条缺失任务")
            else:
                print("✅ 无缺失任务")
        
        # 异步读取所有任务
        查询所有任务SQL = "SELECT * FROM stock_crawl_task_log;"
        await cursor.execute(查询所有任务SQL)
        任务数据 = await cursor.fetchall()
        
        # 构造DataFrame
        列名 = [desc[0] for desc in cursor.description]
        任务数据框 = pd.DataFrame(data=任务数据, columns=列名)
        print(f"✅ 成功读取{len(任务数据框)}条任务记录")
        
    except Exception as e:
        print(f"❌ 任务表处理失败：{str(e)}")
        await 回滚事务(conn)
    
    return 任务数据框

# 重置函数中的建表语句也同步修正（如果有）
async def 重置所有爬取状态(清空任务表: bool = True, 重置股票基础表: bool = True) -> bool:
    """异步重置任务表和股票基础表状态"""
    conn = None
    cursor = None
    操作成功 = False
    
    try:
        conn = await aiomysql.connect(**MYSQL_CONFIG)
        cursor = await conn.cursor()
        
        # 重置股票基础表
        if 重置股票基础表:
            重置股票SQL = """
            UPDATE stock_bar_base 
            SET crawl_status = 'no_crawl', update_time = CURRENT_TIMESTAMP;
            """
            await cursor.execute(重置股票SQL)
            print(f"✅ 股票基础表重置：{cursor.rowcount}条记录")
        
        # 清空任务表
        if 清空任务表:
            清空任务表SQL = "TRUNCATE TABLE stock_crawl_task_log;"
            await cursor.execute(清空任务表SQL)
            print("✅ 任务表已清空")
        
        await 提交事务(conn)
        操作成功 = True
    except Exception as e:
        print(f"❌ 重置失败：{str(e)}")
        if conn:
            await 回滚事务(conn)
    finally:
        # 修复判空逻辑
        if cursor:
            try:
                if not cursor.closed:
                    await cursor.close()
            except Exception as e:
                print(f"❌ 关闭游标失败：{str(e)}")
        if conn:
            try:
                if not conn.closed:
                    await conn.close()
            except Exception as e:
                print(f"❌ 关闭连接失败：{str(e)}")
    return 操作成功