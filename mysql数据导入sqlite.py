import pymysql
import sqlite3

# ================= 配置区域 =================
# MySQL 数据库配置
MYSQL_CONFIG = {
    'host': 'cd-cdb-ccx7yca2.sql.tencentcdb.com',      # 例如：'localhost' 或 '127.0.0.1'
    'port': 28752,             # 默认端口
    'user': 'root',           # 用户名
    'password': '13579sh13579@SH',   # 密码
    'db': 'pachong',    # 要导出的数据库名
    'charset': 'utf8mb4'
}

# SQLite 数据库文件路径
SQLITE_DB = 'pachong_001.db'
# ===========================================

def mysql_type_to_sqlite(mysql_type):
    """简单的 MySQL 类型到 SQLite 类型映射"""
    mt = mysql_type.upper()
    if 'INT' in mt or 'INTEGER' in mt or 'BIGINT' in mt or 'TINYINT' in mt or 'SMALLINT' in mt:
        return 'INTEGER'
    elif 'CHAR' in mt or 'TEXT' in mt or 'ENUM' in mt or 'SET' in mt:
        return 'TEXT'
    elif 'FLOAT' in mt or 'DOUBLE' in mt or 'DECIMAL' in mt:
        return 'REAL'
    elif 'BLOB' in mt or 'BINARY' in mt:
        return 'BLOB'
    elif 'DATE' in mt or 'TIME' in mt or 'YEAR' in mt:
        return 'TEXT' # SQLite 没有专门的时间类型，通常存为 TEXT
    else:
        return 'TEXT'

def main():
    print(f"正在连接 MySQL: {MYSQL_CONFIG['host']}/{MYSQL_CONFIG['db']}...")
    
    # 1. 连接 MySQL
    mysql_conn = pymysql.connect(**MYSQL_CONFIG)
    mysql_cursor = mysql_conn.cursor()

    # 2. 连接 SQLite (如果文件不存在会自动创建)
    print(f"正在连接 SQLite: {SQLITE_DB}...")
    sqlite_conn = sqlite3.connect(SQLITE_DB)
    sqlite_cursor = sqlite_conn.cursor()

    try:
        # 获取所有表名
        mysql_cursor.execute("SHOW TABLES")
        tables = [t[0] for t in mysql_cursor.fetchall()]
        print(f"找到 {len(tables)} 个表，准备迁移...\n")

        for table_name in tables:
            print(f"正在处理表: {table_name}")

            # --- 第一步：迁移表结构 ---
            mysql_cursor.execute(f"DESCRIBE `{table_name}`")
            columns = mysql_cursor.fetchall()
            
            # 构建建表 SQL
            col_defs = []
            for col in columns:
                col_name = col[0]
                col_type = col[1]
                sqlite_type = mysql_type_to_sqlite(col_type)
                # 简单处理，保留字段名和类型，暂不处理主键/索引等复杂约束
                col_defs.append(f"`{col_name}` {sqlite_type}")
            
            create_sql = f"CREATE TABLE IF NOT EXISTS `{table_name}` ({', '.join(col_defs)})"
            sqlite_cursor.execute(f"DROP TABLE IF EXISTS `{table_name}`") # 若存在则先删除
            sqlite_cursor.execute(create_sql)

            # --- 第二步：迁移数据 ---
            # 读取 MySQL 数据
            mysql_cursor.execute(f"SELECT * FROM `{table_name}`")
            rows = mysql_cursor.fetchall()
            
            if rows:
                # 构建占位符: (?, ?, ?)
                placeholders = ', '.join(['?'] * len(columns))
                insert_sql = f"INSERT INTO `{table_name}` VALUES ({placeholders})"
                
                # 批量插入 SQLite
                sqlite_cursor.executemany(insert_sql, rows)
                print(f"  -> 成功导入 {len(rows)} 条数据")
            else:
                print(f"  -> 表为空，跳过数据导入")

            sqlite_conn.commit()

        print("\n✅ 所有数据迁移完成！")

    except Exception as e:
        print(f"\n❌ 发生错误: {e}")
        sqlite_conn.rollback()
    finally:
        # 关闭连接
        mysql_cursor.close()
        mysql_conn.close()
        sqlite_cursor.close()
        sqlite_conn.close()

if __name__ == "__main__":
    main()

