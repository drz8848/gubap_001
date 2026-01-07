import aiomysql
try:
    from aiomysql.err import OperationalError, ProgrammingError
except ImportError:
    # 兼容旧版本aiomysql的导入路径
    from aiomysql import OperationalError, ProgrammingError
import warnings 


# 数据库配置（全局复用）
MYSQL_CONFIG = {
    'host': 'cd-cdb-ccx7yca2.sql.tencentcdb.com',
    'port': 28752,
    'user': 'root',
    'password': '13579sh13579@SH',
    'db': 'pachong',  # aiomysql用db而非database
    'charset': 'utf8mb4',
    'autocommit': False  # 手动控制事务
}

warnings.filterwarnings('ignore', category=Warning, module='aiomysql')
warnings.filterwarnings('ignore', message='Integer display width is deprecated')
warnings.filterwarnings('ignore', message="Table 'stock_crawl_task_log' already exists")

async def 建立数据库连接():
    """异步建立数据库连接，返回连接+游标"""
    try:
        conn = await aiomysql.connect(**MYSQL_CONFIG)
        cursor = await conn.cursor()
        print("✅ 异步数据库连接建立成功")
        return conn, cursor
    except (OperationalError, ProgrammingError) as e:
        print(f"❌ 异步数据库连接失败：{str(e)}")
        return None, None
    except Exception as e:
        print(f"❌ 数据库连接异常：{str(e)}")
        return None, None

async def 关闭数据库连接(conn, cursor):
    """异步安全关闭数据库游标和连接"""
    print("\n📌 开始关闭数据库连接...")
    # 修复：先判断conn/cursor是否为None，再判断状态
    try:
        if cursor is not None and not cursor.closed:  # 明确判空+状态
            await cursor.close()
            print("✅ 数据库游标已关闭")
        elif cursor is None:
            print("⚠️  游标为None，无需关闭")
    except Exception as e:
        print(f"❌ 关闭游标失败：{str(e)}")
    
    try:
        if conn is not None and not conn.closed:  # 明确判空+状态
            await conn.close()
            print("✅ 数据库连接已关闭")
        elif conn is None:
            print("⚠️  数据库连接为None，无需关闭")
    except Exception as e:
        print(f"❌ 关闭连接失败：{str(e)}")

async def 提交事务(conn):
    """异步提交事务"""
    if conn is not None and not conn.closed:  # 明确判空
        try:
            await conn.commit()
            return True
        except Exception as e:
            print(f"❌ 事务提交失败：{str(e)}")
            await conn.rollback()
            return False
    return False

async def 回滚事务(conn):
    """异步回滚事务"""
    if conn is not None and not conn.closed:  # 明确判空
        try:
            await conn.rollback()
            return True
        except Exception as e:
            print(f"❌ 事务回滚失败：{str(e)}")
            return False
    return False