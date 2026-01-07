from playwright.async_api import Page
from typing import Optional

async def 获取股吧总页数(
    标签页: Page,
    超时时间: int = 10000
) -> Optional[int]:
    """异步提取股吧总页数（定位省略号后的页码）"""
    try:
        # 异步定位元素
        总页数元素 = 标签页.locator("li.dots + li a.nump")
        await 总页数元素.wait_for(state="visible", timeout=超时时间)
        
        # 异步提取文本
        总页数文本 = await 总页数元素.text_content()
        总页数 = int(总页数文本.strip())
        print(f"✅ 成功获取总页数：{总页数}")
        return 总页数
    except Exception as e:
        print(f"❌ 获取总页数失败：{str(e)}")
        return None