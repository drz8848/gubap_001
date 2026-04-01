# 验证爬虫数据完整性脚本
# 用于检查是否有漏爬的帖子，支持回退已爬页数

import sqlite3
import pandas as pd
from 连接数据库 import DB_PATH
import 打开谷歌网页
import 提取文章列表
import 获取股票文章页数
import datetime


def 获取已爬统计信息(conn) -> pd.DataFrame:
    """获取每只股票的爬取统计信息"""
    统计SQL = """
    SELECT
        stock_code,
        COUNT(*) as 爬取帖子数,
        COUNT(DISTINCT post_url) as 唯一URL数,
        MIN(crawl_time) as 首次爬取时间,
        MAX(crawl_time) as 最后爬取时间
    FROM stock_guba_post
    GROUP BY stock_code
    ORDER BY stock_code;
    """
    return pd.read_sql(统计SQL, conn)


def 获取任务表信息(conn) -> pd.DataFrame:
    """获取任务表的页面进度信息"""
    统计SQL = """
    SELECT
        stock_code,
        crawl_page as 已爬页数,
        crawl_status,
        crawl_start_time,
        crawl_complete_time,
        error_msg
    FROM stock_crawl_task_log
    WHERE crawl_status = 'running'
    ORDER BY stock_code;
    """
    return pd.read_sql(统计SQL, conn)


def 检查重复URL(conn) -> pd.DataFrame:
    """检查是否有重复的帖子URL"""
    重复SQL = """
    SELECT post_url, COUNT(*) as 重复次数
    FROM stock_guba_post
    GROUP BY post_url
    HAVING COUNT(*) > 1;
    """
    return pd.read_sql(重复SQL, conn)


def 检查空字段(conn) -> pd.DataFrame:
    """检查关键字段是否为空"""
    空字段SQL = """
    SELECT stock_code, COUNT(*) as 数量
    FROM stock_guba_post
    WHERE post_url = ''
       OR post_title = ''
       OR post_content IS NULL
       OR post_content = ''
    GROUP BY stock_code
    ORDER BY 数量 DESC;
    """
    return pd.read_sql(空字段SQL, conn)


def 更新已爬页数(conn, 股票代码, 新页数):
    """回退已爬页数到指定值"""
    cursor = conn.cursor()

    # 获取任务ID
    get_id_sql = """
    SELECT id FROM stock_crawl_task_log
    WHERE stock_code = ? AND crawl_status = 'running'
    ORDER BY id DESC
    LIMIT 1;
    """
    cursor.execute(get_id_sql, (股票代码,))
    row = cursor.fetchone()
    if not row:
        print(f"⚠️ 未找到股票{股票代码}的running状态任务")
        return False

    task_id = row[0]

    # 更新爬取页数
    update_sql = "UPDATE stock_crawl_task_log SET crawl_page = ? WHERE id = ?;"
    cursor.execute(update_sql, (新页数, task_id))
    conn.commit()

    print(f"✅ 股票{股票代码}的已爬页数已从{len(row) if row else 0}回退到{新页数}")
    return True


def 验证特定股票(conn, 股票代码, 浏览器上下文, 回退模式=False, 自动回退=None):
    """
    对比网站实际数量与数据库数量，验证已爬取范围

    Args:
        conn: 数据库连接
        股票代码: 要验证的股票代码
        浏览器上下文: Playwright浏览器上下文
        回退模式: 是否启用回退已爬页数功能
        自动回退: None=询问, 'a'=全部同意回退, 'k'=全部拒绝回退

    Returns:
        (是否有遗漏, 遗失帖子数, 遗失页面列表)
    """
    cursor = conn.cursor()

    # 获取数据库中该股票的帖子数
    数据库帖子数SQL = "SELECT COUNT(*) FROM stock_guba_post WHERE stock_code = ?;"
    cursor.execute(数据库帖子数SQL, (股票代码,))
    数据库帖子数 = cursor.fetchone()[0]

    # 获取该股票的已爬页数
    已爬页数SQL = "SELECT crawl_page FROM stock_crawl_task_log WHERE stock_code = ?;"
    cursor.execute(已爬页数SQL, (股票代码,))
    结果 = cursor.fetchone()
    已爬页数 = 结果[0] if 结果 else 0

    # 打开股票股吧首页获取总页数
    首页链接 = f"https://guba.eastmoney.com/list,{股票代码}_1.html"
    标签页 = 打开谷歌网页.打开新标签页(浏览器上下文, 首页链接)
    总页数 = 获取股票文章页数.获取股吧总页数(标签页)
    打开谷歌网页.关闭单个标签页(标签页)

    if not 总页数:
        if 回退模式:
            print(f"\n❌ [{股票代码}] 无法获取总页数，跳过")
        return (False, 0, [])

    if 回退模式:
        print(f"\n🔍 [{股票代码}] 数据库{数据库帖子数}条 | 总{总页数}页 | 已爬{已爬页数}页 | {已爬页数/总页数*100:.2f}%")

    if 已爬页数 == 0:
        if 回退模式:
            print(f"   ⚠️ 尚未开始爬取")
        return (False, 0, [])

    # 验证已爬取范围内的页面
    验证页码范围 = list(range(总页数, 总页数 - 已爬页数, -1))

    总检查帖子数 = 0
    总缺失帖子数 = 0
    缺失页面列表 = {}  # {页码: [缺失URL列表]}

    for 页码 in 验证页码范围:
        测试链接 = f"https://guba.eastmoney.com/list,{股票代码}_{页码}.html"
        测试标签页 = 打开谷歌网页.打开新标签页(浏览器上下文, 测试链接)

        try:
            测试数据框 = 提取文章列表.提取股吧文章列表(测试标签页, 股票代码, 页码)
            页面文章数 = len(测试数据框)
            总检查帖子数 += 页面文章数

            # 检查这些帖子是否都在数据库中
            测试URL列表 = 测试数据框["详情页URL"].tolist()
            当前页缺失 = []
            for url in 测试URL列表:
                cursor.execute("SELECT 1 FROM stock_guba_post WHERE post_url = ? LIMIT 1;", (url,))
                if not cursor.fetchone():
                    当前页缺失.append(url)

            缺失数量 = len(当前页缺失)
            总缺失帖子数 += 缺失数量

            if 缺失数量 > 0:
                缺失页面列表[页码] = 当前页缺失
                if 回退模式:
                    print(f"   ❌ 第{页码}页：缺失{缺失数量}条")
            else:
                if 回退模式 and len(验证页码范围) <= 5:
                    print(f"   ✅ 第{页码}页：完整")

        except Exception as e:
            if 回退模式:
                print(f"   ⚠️ 第{页码}页验证失败：{str(e)}")
        finally:
            打开谷歌网页.关闭单个标签页(测试标签页)

    # 回退逻辑
    if 回退模式 and 总缺失帖子数 > 0:
        找到首缺失页 = min(缺失页面列表.keys())  # 最小页码（最前面的缺失页）
        需回退到的页数 = 总页数 - 找到首缺失页 + 1  # 从缺失页开始重新爬

        print(f"\n   📊 总结：检查{len(验证页码范围)}页，缺失{总缺失帖子数}条，分布在{len(缺失页面列表)}页")
        print(f"   💡 首个缺失页：第{找到首缺失页}页，需回退到 {需回退到的页数}页")

        决定 = 自动回退
        if 决定 is None:
            决定 = input(f"   是否回退？[y=同意回退/a=全部同意/n=拒绝/k=全部拒绝]: ").strip().lower()

        if 决定 == 'a':
            自动回退 = 'a'
        elif 决定 == 'k':
            自动回退 = 'k'

        if 决定 in ['y', 'a']:
            更新已爬页数(conn, 股票代码, 需回退到的页数)
        else:
            print(f"   ⏭️  跳过回退")

    return (总缺失帖子数 > 0, 总缺失帖子数, list(缺失页面列表.keys()))


def 批量验证所有股票():
    """批量验证所有股票"""
    print("\n" + "=" * 60)
    print("🔍 批量验证所有股票")
    print("=" * 60)

    conn = sqlite3.connect(DB_PATH)

    try:
        # 获取所有股票代码
        统计df = 获取已爬统计信息(conn)
        股票代码列表 = 统计df['stock_code'].tolist()

        print(f"\n📊 共发现 {len(股票代码列表)} 只股票，开始验证...")

        # 创建浏览器
        p, 浏览器, 上下文 = 打开谷歌网页.创建浏览器实例(无头模式=False)
        保持标签页 = 打开谷歌网页.打开新标签页(上下文, "https://guba.eastmoney.com")

        统计结果 = {
            '总股票数': len(股票代码列表),
            '有遗漏股票数': 0,
            '无遗漏股票数': 0,
            '已回退股票数': 0
        }

        遗失详情 = []

        for 索引, 股票代码 in enumerate(股票代码列表):
            if (索引 + 1) % 10 == 1:
                print(f"\n📈 进度：{索引 + 1}/{len(股票代码列表)}")

            try:
                有遗漏, 遗失数, 遗失页面 = 验证特定股票(
                    conn, 股票代码, 上下文,
                    回退模式=True,
                    自动回退=None
                )

                if 有遗漏:
                    统计结果['有遗漏股票数'] += 1
                    遗失详情.append({
                        'stock_code': 股票代码,
                        '缺失数': 遗失数,
                        '缺失页面': 遗失页面
                    })
                else:
                    统计结果['无遗漏股票数'] += 1

            except Exception as e:
                print(f"   ❌ [{股票代码}] 验证失败：{str(e)}")

        # 输出总结
        print(f"\n{'='*60}")
        print("📋 批量验证总结")
        print(f"{'='*60}")
        print(f"✅ 总股票数：{统计结果['总股票数']}")
        print(f"✅ 无遗漏：{统计结果['无遗漏股票数']}")
        print(f"⚠️  有遗漏：{统计结果['有遗漏股票数']}")

        if 遗失详情:
            print(f"\n⚠️ 有遗漏的股票列表（前20只）：")
            for 详情 in 遗失详情[:20]:
                print(f"   {详情['stock_code']}: 缺失{详情['缺失数']}条，页面{详情['缺失页面']}")

        print(f"\n{'='*60}")

        打开谷歌网页.关闭单个标签页(保持标签页)
        打开谷歌网页.关闭浏览器实例(p, 浏览器, 上下文)

    finally:
        conn.close()


if __name__ == "__main__":
    print("=" * 60)
    print("🔍 爬虫数据完整性验证")
    print("=" * 60)

    conn = sqlite3.connect(DB_PATH)

    try:
        # 1. 显示爬取统计
        print("\n📊 爬取统计信息：")
        print("-" * 60)
        统计df = 获取已爬统计信息(conn)
        print(统计df.to_string(index=False))

        # 2. 显示任务进度
        print(f"\n📋 任务进度：")
        print("-" * 60)
        任务df = 获取任务表信息(conn)
        print(任务df.to_string(index=False))

        # 3. 检查重复URL
        print(f"\n🔍 重复URL检查：")
        print("-" * 60)
        重复df = 检查重复URL(conn)
        if len(重复df) > 0:
            print(f"❌ 发现{len(重复df)}个重复的帖子URL")
        else:
            print("✅ 没有发现重复的帖子URL")

        # 4. 检查空字段
        print(f"\n🔍 空字段检查：")
        print("-" * 60)
        空字段df = 检查空字段(conn)
        if len(空字段df) > 0:
            print(f"⚠️ 发现{空字段df['数量'].sum()}条记录有空字段")
        else:
            print("✅ 没有发现空字段")

        # 5. 选择验证模式
        print(f"\n{'='*60}")
        print("请选择验证模式：")
        print("1. 验证特定股票（支持回退已爬页数）")
        print("2. 批量验证所有股票（支持回退已爬页数）")
        print("0. 退出")

        模式选择 = input("\n请输入选项 [0/1/2]: ").strip()

        if 模式选择 == '1':
            需要验证 = input("\n输入股票代码（如000001），直接回车跳过: ").strip()
            if 需要验证:
                p, 浏览器, 上下文 = 打开谷歌网页.创建浏览器实例(无头模式=False)
                保持标签页 = 打开谷歌网页.打开新标签页(上下文, "https://guba.eastmoney.com")
                try:
                    print(f"\n🔎 详细验证模式：")
                    print("-" * 60)
                    有遗漏, 遗失数, 遗失页面 = 验证特定股票(
                        conn, 需要验证, 上下文,
                        回退_mode=True,
                        自动回退=None
                    )
                finally:
                    打开谷歌网页.关闭单个标签页(保持标签页)
                    打开谷歌网页.关闭浏览器实例(p, 浏览器, 上下文)

        elif 模式选择 == '2':
            批量验证所有股票()

        print(f"\n{'='*60}")
        print("✅ 验证完成")
        print(f"{'='*60}")

    finally:
        conn.close()
