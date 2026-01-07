from playwright.sync_api import Page
from typing import Tuple, Optional
import re

def 获取文章点赞和发布时间(page: Page, 文章链接: str) -> Tuple[Optional[int], Optional[str]]:
    """
    获取东方财富网2个子域名文章的**点赞数**和**统一格式的发布时间**
    - 核心优化：增加数字校验与容错，处理老文章「点赞」文本无法转换的问题
    - 统一时间格式：YYYY-MM-DD HH:MM:SS
    - 兜底处理：无有效点赞数返回0，无有效时间返回None
    
    参数:
        page: Playwright的Page对象（需已打开目标文章页面）
        文章链接: 文章的URL（用于区分子域名）
    
    返回:
        Tuple[点赞数(整数), 统一格式的发布时间(字符串)]
    """
    try:
        # 1. 处理 guba.eastmoney.com 子域名（容错优化，处理老文章）
        if 文章链接.startswith("https://guba.eastmoney.com/"):
            # -------- 文章发布时间：严格限定父元素，增加容错 --------
            发布时间定位器 = page.locator("div.newsauthor div.time")
            if 发布时间定位器.count() == 0:
                print("⚠️  未找到文章发布时间，返回None")
                发布时间 = None
            else:
                发布时间 = 发布时间定位器.text_content().strip()
                # 补充：清洗空字符串时间
                发布时间 = 发布时间 if 发布时间 else None
            
            # -------- 点赞数：处理「点赞」文本场景 --------
            点赞数定位器 = page.locator("div.newsinter span.likemodule")
            if 点赞数定位器.count() == 0:
                print("⚠️  未找到点赞元素，返回点赞数0")
                点赞数 = 0
            else:
                点赞文本 = 点赞数定位器.text_content().strip()
                # 步骤1：清洗文本（移除空格、特殊字符，仅保留数字）
                清洗后点赞文本 = re.sub(r"[^0-9]", "", 点赞文本)
                # 步骤2：数字校验，无有效数字则返回0（对应「点赞」文本场景）
                if 清洗后点赞文本.isdigit():
                    点赞数 = int(清洗后点赞文本)
                else:
                    print(f"⚠️  无有效点赞数字（原始文本：{点赞文本}），返回点赞数0")
                    点赞数 = 0

            return 点赞数, 发布时间

        # 2. 处理 caifuhao.eastmoney.com 子域名（保留原逻辑，增加轻微容错）
        elif 文章链接.startswith("https://caifuhao.eastmoney.com/"):
            # 定位原始发布时间
            原始时间定位器 = page.locator("div.article-meta span.txt").nth(0)
            if 原始时间定位器.count() == 0:
                print("⚠️  未找到财富号文章发布时间，返回None")
                发布时间 = None
            else:
                原始时间 = 原始时间定位器.text_content().strip()
                # 转换为目标格式
                发布时间 = 原始时间.replace("年", "-").replace("月", "-").replace("日 ", " ") + ":00"

            # 定位点赞数，增加数字校验容错
            点赞定位器 = page.locator("span.zancout.text-primary")
            if 点赞定位器.count() == 0:
                print("⚠️  未找到财富号文章点赞数，返回0")
                点赞数 = 0
            else:
                点赞文本 = 点赞定位器.text_content().strip()
                清洗后点赞文本 = re.sub(r"[^0-9]", "", 点赞文本)
                点赞数 = int(清洗后点赞文本) if 清洗后点赞文本.isdigit() else 0

            return 点赞数, 发布时间

        # 不支持的域名
        else:
            print(f"❌ 不支持的URL域名: {文章链接}")
            return 0, None

    except Exception as e:
        print(f"❌ 获取文章信息失败: {str(e)}")
        return 0, None