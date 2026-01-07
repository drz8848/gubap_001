import pandas as pd
from playwright.async_api import Page
from urllib.parse import urljoin

async def 提取股吧文章列表(
    标签页: Page,
    股票代码: str,
    页码: int
) -> pd.DataFrame:
    """异步提取单页文章列表，包含作者ID/URL"""
    # 异步等待列表加载
    try:
        await 标签页.wait_for_selector("tr.listitem", timeout=30000)
        print(f"✅ 文章列表元素加载完成（股票：{股票代码} 第{页码}页）")
    except Exception as e:
        raise RuntimeError(f"❌ 文章列表加载超时：{str(e)}")

    # 异步获取所有文章行
    文章行列表 = await 标签页.locator("tr.listitem").all()
    if not 文章行列表:
        print(f"⚠️  未找到文章数据（股票：{股票代码} 第{页码}页）")
        return pd.DataFrame()

    # 异步遍历提取数据
    文章数据列表 = []
    基础链接 = 标签页.url
    for 行 in 文章行列表:
        行数据 = {}
        
        # 阅读数（异步提取）
        try:
            阅读文本 = await 行.locator("div.read").text_content()
            行数据["阅读"] = int(阅读文本.strip()) if 阅读文本.strip().isdigit() else 0
        except:
            行数据["阅读"] = 0
        
        # 评论数
        try:
            评论文本 = await 行.locator("div.reply").text_content()
            行数据["评论"] = int(评论文本.strip()) if 评论文本.strip().isdigit() else 0
        except:
            行数据["评论"] = 0
        
        # 标题+详情页URL
        try:
            标题定位器 = 行.locator("div.title a")
            行数据["标题"] = await 标题定位器.text_content() if await 标题定位器.is_visible() else ""
            相对链接 = await 标题定位器.get_attribute("href")
            行数据["详情页URL"] = urljoin(基础链接, 相对链接) if 相对链接 else ""
        except:
            行数据["标题"] = ""
            行数据["详情页URL"] = ""
        
        # 作者+作者ID+URL
        try:
            作者定位器 = 行.locator("div.author.cl a")
            行数据["作者"] = await 作者定位器.text_content() if await 作者定位器.is_visible() else ""
            作者相对链接 = await 作者定位器.get_attribute("href")
            行数据["作者URL"] = urljoin(基础链接, 作者相对链接) if 作者相对链接 else ""
            行数据["作者ID"] = 行数据["作者URL"].split("/")[-1] if 行数据["作者URL"] and 行数据["作者URL"].split("/")[-1].isdigit() else ""
        except:
            行数据["作者"] = ""
            行数据["作者URL"] = ""
            行数据["作者ID"] = ""
        
        # 最后更新时间
        try:
            更新定位器 = 行.locator("div.update.mod_time")
            行数据["最后更新"] = await 更新定位器.text_content() if await 更新定位器.is_visible() else ""
        except:
            行数据["最后更新"] = ""
        
        文章数据列表.append(行数据)

    # 构建DataFrame
    数据框 = pd.DataFrame(文章数据列表)
    数据框.insert(0, "stock_code", 股票代码)

    print(f"✅ 数据提取完成：共{len(数据框)}条文章（股票：{股票代码} 第{页码}页）")
    return 数据框