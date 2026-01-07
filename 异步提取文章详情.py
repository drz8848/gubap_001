from playwright.async_api import Page
from typing import Tuple, Optional
import re

async def 获取文章点赞和发布时间及正文(page: Page, 文章链接: str) -> Tuple[Optional[int], Optional[str], str]:
    """异步提取点赞数、发布时间、帖子正文"""
    try:
        正文内容 = ""
        
        # 处理guba子域名
        if 文章链接.startswith("https://guba.eastmoney.com/"):
            # 发布时间
            发布时间定位器 = page.locator("div.newsauthor div.time")
            if await 发布时间定位器.count() == 0:
                print("⚠️  未找到发布时间，返回None")
                发布时间 = None
            else:
                发布时间 = (await 发布时间定位器.text_content()).strip()
                发布时间 = 发布时间 if 发布时间 else None
            
            # 点赞数
            点赞数定位器 = page.locator("div.newsinter span.likemodule")
            if await 点赞数定位器.count() == 0:
                print("⚠️  未找到点赞元素，返回0")
                点赞数 = 0
            else:
                点赞文本 = (await 点赞数定位器.text_content()).strip()
                清洗后点赞文本 = re.sub(r"[^0-9]", "", 点赞文本)
                点赞数 = int(清洗后点赞文本) if 清洗后点赞文本.isdigit() else 0
            
            # 正文
            try:
                正文定位器 = page.locator("div.newstext")
                if await 正文定位器.count() > 0:
                    正文原始内容 = (await 正文定位器.text_content()).strip()
                    正文内容 = "\n".join([line.strip() for line in 正文原始内容.split("\n") if line.strip()])
                    调试正文 = 正文内容[:100] + "..." if len(正文内容) > 100 else 正文内容
                    print(f"📝 提取到guba正文：{调试正文}")
                else:
                    print("⚠️  未找到guba正文")
            except Exception as e:
                print(f"❌ 提取guba正文异常：{str(e)}")
                正文内容 = ""
        
        # 处理财富号子域名
        elif 文章链接.startswith("https://caifuhao.eastmoney.com/"):
            # 发布时间
            原始时间定位器 = page.locator("div.article-meta span.txt").nth(0)
            if await 原始时间定位器.count() == 0:
                print("⚠️  未找到财富号发布时间")
                发布时间 = None
            else:
                原始时间 = (await 原始时间定位器.text_content()).strip()
                发布时间 = 原始时间.replace("年", "-").replace("月", "-").replace("日 ", " ") + ":00"
            
            # 点赞数
            点赞定位器 = page.locator("span.zancout.text-primary")
            if await 点赞定位器.count() == 0:
                print("⚠️  未找到财富号点赞数")
                点赞数 = 0
            else:
                点赞文本 = (await 点赞定位器.text_content()).strip()
                清洗后点赞文本 = re.sub(r"[^0-9]", "", 点赞文本)
                点赞数 = int(清洗后点赞文本) if 清洗后点赞文本.isdigit() else 0
            
            # 正文
            try:
                正文定位器 = page.locator("div.article-body")
                if await 正文定位器.count() > 0:
                    正文原始内容 = (await 正文定位器.text_content()).strip()
                    正文内容 = "\n".join([line.strip() for line in 正文原始内容.split("\n") if line.strip()])
                    调试正文 = 正文内容[:100] + "..." if len(正文内容) > 100 else 正文内容
                    print(f"📝 提取到财富号正文：{调试正文}")
                else:
                    print("⚠️  未找到财富号正文")
            except Exception as e:
                print(f"❌ 提取财富号正文异常：{str(e)}")
                正文内容 = ""
        else:
            print(f"❌ 不支持的域名: {文章链接}")
            return 0, None, ""

        return 点赞数, 发布时间, 正文内容

    except Exception as e:
        print(f"❌ 获取文章信息失败: {str(e)}")
        return 0, None, ""