import asyncio
import pandas as pd
import datetime
import nest_asyncio  

# 应用nest_asyncio，兼容Spyder/Jupyter
nest_asyncio.apply()

# 导入异步模块
from 异步数据库连接 import 建立数据库连接, 关闭数据库连接
from 异步任务列表管理 import 初始化并读取爬取任务列表
from 异步浏览器操作 import 创建浏览器实例, 打开新标签页, 关闭单个标签页, 关闭浏览器实例
from 异步获取股吧总页数 import 获取股吧总页数
from 异步提取文章列表 import 提取股吧文章列表
from 异步提取文章详情 import 获取文章点赞和发布时间及正文
from 异步写入帖子数据 import 写入单篇帖子数据
from 异步更新爬取进度 import 更新任务已爬页数

# 控制变量
控制变量1 = 0  # 股票数量
控制变量2 = 0  # 页码数量
控制变量3 = 0  # 文章数量

async def 主爬取逻辑():
    """异步核心爬取逻辑"""
    # 1. 初始化数据库连接（主程序唯一创建连接）
    数据库连接, 数据库游标 = await 建立数据库连接()
    if not (数据库连接 and 数据库游标):
        print("❌ 数据库连接失败，程序终止")
        return
    
    # 初始化浏览器相关变量为None（避免未定义）
    p, 浏览器, 上下文, 保持标签页 = None, None, None, None
    
    try:
        # 2. 获取任务列表【修复】传入主程序的数据库连接和游标，避免重复创建
        任务数据框 = await 初始化并读取爬取任务列表(数据库连接, 数据库游标, True)
        print(f"\n📊 任务列表加载完成，共{len(任务数据框)}条任务")
        print(任务数据框.head())
        
        # 3. 创建浏览器实例（增加异常捕获）
        try:
            p, 浏览器, 上下文 = await 创建浏览器实例(无头模式=False)
            保持标签页 = await 打开新标签页(上下文, "https://guba.eastmoney.com/list,000001_1.html")
        except Exception as e:
            print(f"❌ 创建浏览器实例失败：{str(e)}")
            return  # 浏览器创建失败则终止
        
        # 4. 遍历股票代码（增加非空判断）
        目标列 = "stock_code"
        if 目标列 not in 任务数据框.columns:
            print(f"❌ 任务数据框中无{目标列}列")
            return
        
        # 只取前N条测试（避免一次性爬取5902条）
        测试股票列表 = 任务数据框[目标列].head(2).tolist()
        for 股票代码 in 测试股票列表:
            global 控制变量1, 控制变量2, 控制变量3
            控制变量1 += 1
            第一层网页链接 = f"https://guba.eastmoney.com/list,{股票代码}_1.html"
            
            try:
                # 4.1 打开股票首页，获取总页数
                主标签页 = await 打开新标签页(上下文, 第一层网页链接)
                if not 主标签页:  # 增加标签页判空
                    print(f"⚠️  无法打开{股票代码}首页，跳过")
                    continue
                
                总页数 = await 获取股吧总页数(主标签页)
                if not 总页数:
                    print(f"⚠️  无法获取{股票代码}总页数，跳过")
                    await 关闭单个标签页(主标签页)
                    continue
                print(f"📌 股票{股票代码}总页数：{总页数}，开始从后往前爬取")
                
                # 4.2 遍历页码（从后往前）
                控制变量2 = 0
                # 只取最后2页测试
                起始页码 = max(1, 总页数 - 1)
                for 页码 in range(总页数, 起始页码 - 1, -1):
                    控制变量2 += 1
                    try:
                        # 打开分页标签页
                        第二层网页链接 = f"https://guba.eastmoney.com/list,{股票代码}_{页码}.html"
                        分页标签页 = await 打开新标签页(上下文, 第二层网页链接)
                        if not 分页标签页:
                            print(f"⚠️  无法打开{股票代码}第{页码}页，跳过")
                            continue
                        
                        # 提取文章列表
                        文章数据框 = await 提取股吧文章列表(分页标签页, 股票代码, 页码)
                        if 文章数据框.empty:
                            await 关闭单个标签页(分页标签页)
                            continue
                        
                        # 4.3 遍历文章详情（只取前2条测试）
                        控制变量3 = 0
                        详情页URL列 = "详情页URL"
                        if 详情页URL列 not in 文章数据框.columns:
                            print(f"❌ 文章数据框中无{详情页URL列}列")
                            await 关闭单个标签页(分页标签页)
                            continue
                        
                        for 详情页链接 in 文章数据框[详情页URL列].head(2).tolist():
                            控制变量3 += 1
                            try:
                                # 打开详情页
                                最后标签页 = await 打开新标签页(上下文, 详情页链接)
                                if not 最后标签页:
                                    print(f"⚠️  无法打开详情页{详情页链接[:50]}，跳过")
                                    continue
                                
                                # 提取详情数据
                                点赞数, 发布时间, 正文内容 = await 获取文章点赞和发布时间及正文(最后标签页, 详情页链接)
                                
                                # 构造数据字典
                                文章行 = 文章数据框[文章数据框[详情页URL列] == 详情页链接].iloc[0]
                                帖子数据 = {
                                    "stock_code": 股票代码,
                                    "post_title": 文章行["标题"],
                                    "post_url": 详情页链接,
                                    "post_read_count": str(文章行["阅读"]),
                                    "post_comment_count": str(文章行["评论"]),
                                    "like_num": 点赞数,
                                    "author_name": 文章行["作者"],
                                    "author_id": 文章行["作者ID"],
                                    "author_url": 文章行["作者URL"],
                                    "post_update_time": 文章行["最后更新"],
                                    "post_create_time": 发布时间,
                                    "post_content": 正文内容,
                                    "crawl_time": datetime.datetime.now()
                                }
                                
                                # 写入数据库
                                await 写入单篇帖子数据(数据库连接, 数据库游标, 帖子数据)
                                
                                # 关闭详情页
                                await 关闭单个标签页(最后标签页)
                                
                            except Exception as e:
                                print(f"\n❌ 文章详情页失败：{str(e)}")
                                try:
                                    await 关闭单个标签页(最后标签页)
                                except:
                                    pass
                            
                            # 限制单页文章数
                            if 控制变量3 == 90:  
                                break
                        
                        # 更新爬取进度
                        await 更新任务已爬页数(数据库连接, 数据库游标, 股票代码, 总页数, 页码)
                        
                        # 关闭分页标签页
                        await 关闭单个标签页(分页标签页)
                        
                    except Exception as e:
                        print(f"\n❌ 分页列表页失败：{str(e)}")
                        try:
                            await 关闭单个标签页(分页标签页)
                        except:
                            pass
                    
                    # 限制页码数
                    if 控制变量2 == 2:
                        break
                
                # 关闭股票首页标签页
                await 关闭单个标签页(主标签页)
                
            except Exception as e:
                print(f"\n❌ 股票首页失败：{str(e)}")
                try:
                    await 关闭单个标签页(主标签页)
                except:
                    pass
            
            # 限制股票数
            if 控制变量1 == 2:
                break
        
        # 延迟1秒
        await asyncio.sleep(1)
    
    except Exception as e:
        print(f"\n❌ 主程序失败：{str(e)}")
        import traceback
        traceback.print_exc()  # 打印完整异常栈，方便调试
    finally:
        # 关闭浏览器资源（增加判空）
        try:
            if 保持标签页:
                await 关闭单个标签页(保持标签页)
        except Exception as e:
            print(f"❌ 关闭保持标签页失败：{str(e)}")
        
        try:
            if p and 浏览器 and 上下文:  
                await 关闭浏览器实例(p, 浏览器, 上下文)
        except Exception as e:
            print(f"❌ 关闭浏览器实例失败：{str(e)}")
        
        # 关闭数据库连接（此时conn是主程序创建的，非None）
        await 关闭数据库连接(数据库连接, 数据库游标)
        print("\n🎉 异步爬虫运行结束，所有资源已释放")

def run_async_main():
    """兼容交互式环境和普通环境的异步运行函数"""
    try:
        # 检查是否已有运行中的事件循环
        loop = asyncio.get_event_loop()
        if loop.is_running():
            # 已有循环，直接提交任务
            task = loop.create_task(主爬取逻辑())
            # 等待任务完成
            loop.run_until_complete(task)
            return
    except RuntimeError:
        # 没有运行中的循环，使用asyncio.run()
        pass
    
    # 普通环境下运行
    asyncio.run(主爬取逻辑())

if __name__ == "__main__":
    run_async_main()