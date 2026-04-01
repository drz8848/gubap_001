from playwright.sync_api import Page
from typing import Optional

def 获取股吧总页数(
    标签页: Page,
    超时时间: int = 10000
) -> Optional[int]:
    """
    修正：定位“省略号后的总页数按钮”，提取东方财富网股吧的总页数
    :param 标签页: 已加载完成的股吧页面Page对象
    :param 超时时间: 元素等待超时时间（毫秒）
    :return: 总页数（int），失败则返回None
    """
    try:
        # 核心修正：定位“省略号(li.dots)后面相邻li中的a.nump”
        总页数元素 = 标签页.locator(
            "li.dots + li a.nump"
        )
        
        # 等待元素加载并可见
        总页数元素.wait_for(state="visible", timeout=超时时间)
        
        # 提取元素文本并转换为整数
        总页数文本 = 总页数元素.text_content().strip()
        总页数 = int(总页数文本)
        print(f"✅ 成功获取总页数：{总页数}")
        return 总页数
    
    except Exception as e:
        print(f"❌ 获取总页数失败：{str(e)}")
        return None