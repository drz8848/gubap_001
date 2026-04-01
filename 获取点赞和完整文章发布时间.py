from playwright.sync_api import Page
from typing import Tuple, Optional
import re

def 获取文章点赞和发布时间及正文(page: Page, 文章链接: str) -> Tuple[Optional[int], Optional[str], str]:
    """
    获取东方财富网2个子域名文章的**点赞数**、**统一格式发布时间**、**帖子正文**
    - 适配两个子域名的正文元素：
      - guba.eastmoney.com：正文在 div.newstext
      - caifuhao.eastmoney.com：正文在 div.article-body
    - 统一时间格式：YYYY-MM-DD HH:MM:SS
    - 兜底处理：无有效点赞数返回0，无有效时间返回None，无正文返回空字符串
    
    参数:
        page: Playwright的Page对象（需已打开目标文章页面）
        文章链接: 文章的URL（用于区分子域名）
    
    返回:
        Tuple[点赞数(整数), 统一格式的发布时间(字符串), 帖子正文(字符串)]
    """
    try:
        # 初始化正文内容（默认空字符串）
        正文内容 = ""
        
        # 1. 处理 guba.eastmoney.com 子域名
        if 文章链接.startswith("https://guba.eastmoney.com/"):
            # -------- 文章发布时间 --------
            发布时间定位器 = page.locator("div.newsauthor div.time")
            if 发布时间定位器.count() == 0:
                print("⚠️  未找到文章发布时间，返回None")
                发布时间 = None
            else:
                发布时间 = 发布时间定位器.text_content().strip()
                发布时间 = 发布时间 if 发布时间 else None
            
            # -------- 点赞数 --------
            点赞数定位器 = page.locator("div.newsinter span.likemodule")
            if 点赞数定位器.count() == 0:
                print("⚠️  未找到点赞元素，返回点赞数0")
                点赞数 = 0
            else:
                点赞文本 = 点赞数定位器.text_content().strip()
                清洗后点赞文本 = re.sub(r"[^0-9]", "", 点赞文本)
                点赞数 = int(清洗后点赞文本) if 清洗后点赞文本.isdigit() else 0
            
            # -------- 帖子正文（适配guba的div.newstext） --------
            正文内容 = ""  # 初始化为空字符串，避免None
            try:
                正文定位器 = page.locator("div.newstext")
                if 正文定位器.count() > 0:
                    正文原始内容 = 正文定位器.text_content().strip()
                    # 清洗多余换行和空格，保留有效内容
                    正文内容 = "\n".join([line.strip() for line in 正文原始内容.split("\n") if line.strip()])
                    # 打印提取到的正文（前100字），方便调试
                    调试正文 = 正文内容[:100] + "..." if len(正文内容) > 100 else 正文内容
                    print(f"📝 提取到guba文章正文：{调试正文}")
                else:
                    print("⚠️  未找到guba文章正文，返回空字符串")
            except Exception as e:
                print(f"❌ 提取guba文章正文时发生异常：{str(e)}")
                正文内容 = ""  # 异常时强制设为空字符串

        # 2. 处理 caifuhao.eastmoney.com 子域名
        elif 文章链接.startswith("https://caifuhao.eastmoney.com/"):
            # -------- 文章发布时间 --------
            原始时间定位器 = page.locator("div.article-meta span.txt").nth(0)
            if 原始时间定位器.count() == 0:
                print("⚠️  未找到财富号文章发布时间，返回None")
                发布时间 = None
            else:
                原始时间 = 原始时间定位器.text_content().strip()
                发布时间 = 原始时间.replace("年", "-").replace("月", "-").replace("日 ", " ") + ":00"
            
            # -------- 点赞数 --------
            点赞定位器 = page.locator("span.zancout.text-primary")
            if 点赞定位器.count() == 0:
                print("⚠️  未找到财富号文章点赞数，返回0")
                点赞数 = 0
            else:
                点赞文本 = 点赞定位器.text_content().strip()
                清洗后点赞文本 = re.sub(r"[^0-9]", "", 点赞文本)
                点赞数 = int(清洗后点赞文本) if 清洗后点赞文本.isdigit() else 0
            
            # -------- 帖子正文（适配caifuhao的div.article-body） --------
            正文内容 = ""
            try:
                正文定位器 = page.locator("div.article-body")
                if 正文定位器.count() > 0:
                    正文原始内容 = 正文定位器.text_content().strip()
                    # 清洗多余换行和空格，保留有效内容
                    正文内容 = "\n".join([line.strip() for line in 正文原始内容.split("\n") if line.strip()])
                    调试正文 = 正文内容[:100] + "..." if len(正文内容) > 100 else 正文内容
                    print(f"📝 提取到财富号文章正文：{调试正文}")
                else:
                    print("⚠️  未找到财富号文章正文，返回空字符串")
            except Exception as e:
                print(f"❌ 提取财富号文章正文时发生异常：{str(e)}")
                正文内容 = ""

        # 不支持的域名
        else:
            print(f"❌ 不支持的URL域名: {文章链接}")
            return 0, None, ""

        return 点赞数, 发布时间, 正文内容

    except Exception as e:
        print(f"❌ 获取文章信息失败: {str(e)}")
        return 0, None, ""