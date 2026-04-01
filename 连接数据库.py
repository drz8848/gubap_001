import sqlite3
from sqlite3 import OperationalError, ProgrammingError

# 数据库文件路径（.db 文件）
DB_PATH = './pachong_001.db'

def 建立数据库连接():
    """
    建立数据库连接并返回连接对象和游标对象
    说明：连接创建后不主动关闭，直到程序结束/报错时调用「关闭数据库连接」释放
    返回：(conn, cursor) 数据库连接对象、游标对象
    异常：返回(None, None)并打印错误信息
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        # 启用外键约束
        conn.execute("PRAGMA foreign_keys = ON")
        cursor = conn.cursor()
        print(f"✅ 数据库连接建立成功: {DB_PATH}")
        return conn, cursor
    except (OperationalError, ProgrammingError) as e:
        print(f"❌ 数据库连接失败：{str(e)}")
        return None, None
    except Exception as e:
        print(f"❌ 数据库连接异常：{str(e)}")
        return None, None

def 关闭数据库连接(conn, cursor):
    """
    安全关闭数据库游标和连接，释放资源
    参数：
        conn: 数据库连接对象
        cursor: 数据库游标对象
    """
    print("\n📌 开始关闭数据库连接...")
    try:
        if cursor:
            cursor.close()
            print("✅ 数据库游标已关闭")
    except Exception as e:
        print(f"❌ 关闭游标失败：{str(e)}")
    
    try:
        if conn:
            conn.close()
            print("✅ 数据库连接已关闭")
    except Exception as e:
        print(f"❌ 关闭连接失败：{str(e)}")