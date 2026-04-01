import pandas as pd
from playwright.sync_api import Page
from urllib.parse import urljoin

def 提取股吧文章列表(
    标签页: Page,
    股票代码: str,
    页码: int
) -> pd.DataFrame:
    """
    从Playwright的Page实例中提取股吧文章列表数据，新增作者ID/作者URL字段
    Args:
        标签页: Playwright的Page实例（已打开目标股吧页面）
        股票代码: 股票代码（如"000001"）
        页码: 页码（用于DataFrame标识）
    Returns:
        pd.DataFrame: 包含stock_code、阅读/评论/标题/作者/作者ID/作者URL/最后更新/详情页URL的数据表
    """
    # 1. 等待文章列表加载（确保页面元素渲染完成）
    try:
        标签页.wait_for_selector("tr.listitem", timeout=30000)
        print(f"✅ 文章列表元素加载完成（股票：{股票代码} 第{页码}页）")
    except Exception as e:
        raise RuntimeError(f"❌ 文章列表加载超时：{str(e)}")

    # 2. 提取所有文章行数据
    文章行列表 = 标签页.locator("tr.listitem").all()
    if not 文章行列表:
        print(f"⚠️  未找到文章数据（股票：{股票代码} 第{页码}页）")
        return pd.DataFrame()

    # 3. 遍历行，提取每一条文章的字段（新增作者ID/URL）
    文章数据列表 = []
    基础链接 = 标签页.url  # 页面基础URL，用于拼接绝对路径
    for 行 in 文章行列表:
        行数据 = {}
        
        # 提取阅读数（容错：元素不存在/非数字则设为0）
        try:
            阅读文本 = 行.locator("div.read").text_content().strip()
            行数据["阅读"] = int(阅读文本) if 阅读文本.isdigit() else 0
        except:
            行数据["阅读"] = 0
        
        # 提取评论数（同理容错）
        try:
            评论文本 = 行.locator("div.reply").text_content().strip()
            行数据["评论"] = int(评论文本) if 评论文本.isdigit() else 0
        except:
            行数据["评论"] = 0
        
        # 提取标题 + 详情页URL（URL拼接为绝对路径）
        try:
            标题定位器 = 行.locator("div.title a")
            行数据["标题"] = 标题定位器.text_content().strip() if 标题定位器.is_visible() else ""
            # 相对URL转绝对URL
            相对链接 = 标题定位器.get_attribute("href")
            行数据["详情页URL"] = urljoin(基础链接, 相对链接) if 相对链接 else ""
        except:
            行数据["标题"] = ""
            行数据["详情页URL"] = ""
        
        # 提取作者、作者URL、作者ID（核心新增逻辑）
        try:
            作者定位器 = 行.locator("div.author.cl a")
            行数据["作者"] = 作者定位器.text_content().strip() if 作者定位器.is_visible() else ""
            # 作者URL（相对链接转绝对链接）
            作者相对链接 = 作者定位器.get_attribute("href")
            行数据["作者URL"] = urljoin(基础链接, 作者相对链接) if 作者相对链接 else ""
            # 作者ID（提取URL最后一段的数字串）
            行数据["作者ID"] = ""
            if 行数据["作者URL"]:
                作者_url片段 = 行数据["作者URL"].split("/")[-1]
                行数据["作者ID"] = 作者_url片段 if 作者_url片段.isdigit() else ""
        except:
            行数据["作者"] = ""
            行数据["作者URL"] = ""
            行数据["作者ID"] = ""
        
        # 提取最后更新时间
        try:
            更新定位器 = 行.locator("div.update.mod_time")
            行数据["最后更新"] = 更新定位器.text_content().strip() if 更新定位器.is_visible() else ""
        except:
            行数据["最后更新"] = ""
        
        文章数据列表.append(行数据)

    # 4. 构建DataFrame并插入stock_code列
    数据框 = pd.DataFrame(文章数据列表)
    # 在第1列插入stock_code（所有行值为传入的股票代码）
    数据框.insert(0, "stock_code", 股票代码)

    print(f"✅ 数据提取完成：共{len(数据框)}条文章（股票：{股票代码} 第{页码}页）")
    return 数据框