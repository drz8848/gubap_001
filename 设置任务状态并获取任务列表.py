import sqlite3
import pandas as pd
from sqlite3 import OperationalError, ProgrammingError
import datetime
from 连接数据库 import DB_PATH

def 初始化并读取爬取任务列表(从股票基础表补充任务: bool = True) -> pd.DataFrame:
    """
    修复Bug：仅新增「股票基础表有但任务表没有」的任务，避免重复
    调整返回：返回pandas DataFrame（替代原有多维列表）
    核心功能：创建（若不存在）任务表 → 补充缺失任务 → 读取任务表数据
    :param 从股票基础表补充任务: 是否从stock_bar_base补充缺失任务（默认True）
    :return: 包含任务表所有数据的pandas DataFrame
    """
    创建任务表SQL = """
    CREATE TABLE IF NOT EXISTS stock_crawl_task_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        task_name TEXT NOT NULL,
        stock_code TEXT NOT NULL,
        guba_list_url TEXT NOT NULL,
        crawl_page INTEGER DEFAULT 0,
        last_crawl_post_time DATETIME,
        crawl_status TEXT DEFAULT 'running',
        error_msg TEXT,
        crawl_start_time DATETIME DEFAULT CURRENT_TIMESTAMP,
        crawl_complete_time DATETIME
    )
    """

    conn = None
    cursor = None
    任务数据框 = pd.DataFrame()

    try:
        # 1. 建立数据库连接
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        print("成功连接SQLite数据库，开始处理任务状态管理表")

        # 2. 创建任务状态管理表（若不存在）
        cursor.execute("PRAGMA foreign_keys = ON")
        cursor.execute(创建任务表SQL)
        conn.commit()
        print("stock_crawl_task_log表创建/验证完成")

        # 3. 补充「股票基础表有但任务表没有」的待爬取任务
        if 从股票基础表补充任务:
            # 3.1 先获取任务表中已存在的股票代码（去重）
            查询已有股票SQL = "SELECT DISTINCT stock_code FROM stock_crawl_task_log;"
            cursor.execute(查询已有股票SQL)
            已有股票代码 = [row[0] for row in cursor.fetchall()]

            # 3.2 读取「股票基础表中待爬取 + 任务表中不存在」的股票数据
            if 已有股票代码:
                占位符 = ','.join(['?' for _ in 已有股票代码])
                查询股票SQL = f"""
                SELECT stock_code, guba_list_url FROM stock_bar_base
                WHERE crawl_status = 'no_crawl'
                AND stock_code NOT IN ({占位符});
                """
                cursor.execute(查询股票SQL, 已有股票代码)
            else:
                # 若任务表为空，直接读取所有待爬取股票
                查询股票SQL = """
                SELECT stock_code, guba_list_url FROM stock_bar_base
                WHERE crawl_status = 'no_crawl';
                """
                cursor.execute(查询股票SQL)
            股票数据 = cursor.fetchall()

            if 股票数据:
                # 3.3 插入缺失的任务（任务名格式：日期_股票代码）
                当前日期 = datetime.datetime.now().strftime("%Y%m%d")
                插入任务SQL = """
                INSERT INTO stock_crawl_task_log (
                    task_name, stock_code, guba_list_url, crawl_status
                ) VALUES (?, ?, ?, ?);
                """

                任务计数 = 0
                for 股票代码, 股吧列表链接 in 股票数据:
                    任务名称 = f"{当前日期}_guba_crawl_{股票代码}"
                    参数 = (任务名称, 股票代码, 股吧列表链接, "running")
                    cursor.execute(插入任务SQL, 参数)
                    任务计数 += 1

                conn.commit()
                print(f"成功补充{任务计数}条缺失的爬取任务")
            else:
                print("无缺失任务（任务表已包含所有待爬取股票）")

        # 4. 读取任务表数据，转换为pandas DataFrame
        查询所有任务SQL = "SELECT * FROM stock_crawl_task_log;"
        cursor.execute(查询所有任务SQL)
        任务数据 = cursor.fetchall()

        # 提取表头（字段名）
        列名 = [desc[0] for desc in cursor.description]

        # 构造DataFrame
        任务数据框 = pd.DataFrame(data=任务数据, columns=列名)

        print(f"成功读取任务表，共{len(任务数据框)}条任务记录")
    except OperationalError as e:
        print(f"错误：SQLite连接失败 - {str(e)}")
        if conn:
            conn.rollback()
    except ProgrammingError as e:
        print(f"错误：SQL执行失败 - {str(e)}")
        if conn:
            conn.rollback()
    except Exception as e:
        print(f"错误：任务表处理失败 - {str(e)}")
        if conn:
            conn.rollback()
    finally:
        # 关闭连接
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        print("SQLite连接已关闭")

    return 任务数据框

def 重置所有爬取状态(清空任务表: bool = True, 重置股票基础表: bool = True) -> bool:
    """
    调整逻辑：直接清空任务表 + 重置股票基础表为待爬取状态
    :param 清空任务表: 是否清空任务表（默认True）
    :param 重置股票基础表: 是否重置股票基础表状态（默认True）
    :return: 重置是否成功
    """
    conn = None
    cursor = None
    操作成功 = False

    try:
        # 1. 建立数据库连接
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        print("成功连接SQLite，开始执行重置")

        # 2. 重置股票基础表：所有股票恢复为「待爬取」
        if 重置股票基础表:
            重置股票SQL = """
            UPDATE stock_bar_base
            SET crawl_status = 'no_crawl', update_time = CURRENT_TIMESTAMP;
            """
            cursor.execute(重置股票SQL)
            print(f"股票基础表已重置：{cursor.rowcount}条记录恢复为待爬取")

        # 3. 清空任务表
        if 清空任务表:
            清空任务表SQL = "DELETE FROM stock_crawl_task_log;"
            cursor.execute(清空任务表SQL)
            # 重置自增ID
            cursor.execute("DELETE FROM sqlite_sequence WHERE name='stock_crawl_task_log';")
            print("任务表已清空：所有历史任务记录已删除")

        # 4. 提交事务
        conn.commit()
        print("所有重置操作完成")
        操作成功 = True
    except OperationalError as e:
        print(f"错误：SQLite连接失败 - {str(e)}")
        if conn:
            conn.rollback()
    except ProgrammingError as e:
        print(f"错误：SQL执行失败 - {str(e)}")
        if conn:
            conn.rollback()
    except Exception as e:
        print(f"错误：重置失败 - {str(e)}")
        if conn:
            conn.rollback()
    finally:
        # 关闭连接
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        print("SQLite连接已关闭")

    return 操作成功
