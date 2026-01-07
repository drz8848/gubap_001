from playwright.async_api import async_playwright
from typing import Tuple
import asyncio

async def 创建浏览器实例(
    浏览器类型: str = "chromium",
    无头模式: bool = False
) -> Tuple[any, any, any]:
    """异步创建浏览器实例（Playwright异步版）"""
    # 启动异步Playwright
    p = await async_playwright().start()
    
    # 验证浏览器类型
    支持的浏览器 = ["chromium", "firefox", "webkit"]
    if 浏览器类型 not in 支持的浏览器:
        raise ValueError(f"浏览器类型不合法，可选值：{支持的浏览器}")
    
    # Chrome启动参数（异步版不变）
    浏览器启动参数 = [
        "--no-sandbox",
        "--start-fullscreen",
        "--window-position=0,0",
        "--disable-popup-positioning",
        "--start-maximized"
    ]
    
    # 异步启动浏览器
    浏览器 = await getattr(p, 浏览器类型).launch(
        headless=无头模式,
        args=浏览器启动参数
    )
    
    # 异步创建上下文
    上下文 = await 浏览器.new_context()
    
    print(f"✅ 异步浏览器实例创建成功（{浏览器类型}），已全屏打开窗口")
    return p, 浏览器, 上下文

async def 打开新标签页(
    上下文: any,
    目标链接: str
) -> any:
    """异步打开新标签页，处理身份验证"""
    try:
        新标签页 = await 上下文.new_page()
        
        print(f"\n📌 正在异步打开新标签页：{目标链接}")
        await 新标签页.goto(
            url=目标链接,
            wait_until="domcontentloaded",
            timeout=60000
        )
        
        # 通用身份核实检测（异步版）
        验证关键词 = ["身份核实", "验证", "安全验证", "人机验证"]
        最大等待次数 = 5
        验证等待计数 = 0
        
        while 验证等待计数 < 最大等待次数:
            当前页面标题 = await 新标签页.title()
            当前页面URL = 新标签页.url
            
            触发验证 = any(关键词 in 当前页面标题 for 关键词 in 验证关键词) and \
                        "eastmoney.com" in 当前页面URL
            
            if 触发验证:
                print(f"\n⚠️  触发通用身份核实（链接：{目标链接[:50]}...）")
                print(f"✅ 页面已保持打开，请手动完成验证后按回车...")
                input()  # 阻塞等待用户操作（异步中仅此处同步，不影响核心逻辑）
                await asyncio.sleep(3)
                try:
                    await 新标签页.reload(wait_until="domcontentloaded", timeout=60000)
                except:
                    pass
                验证等待计数 += 1
            else:
                break
        
        if 验证等待计数 >= 最大等待次数:
            print(f"❌ 身份核实超时，无法打开标签页")
            await 新标签页.close()
            return None
        
        print(f"✅ 新标签页打开成功：{await 新标签页.title()}")
        return 新标签页
    except Exception as e:
        print(f"❌ 异步打开标签页失败：{str(e)}")
        try:
            await 新标签页.close()
        except:
            pass
        raise e

async def 关闭单个标签页(标签页: any) -> None:
    """异步关闭单个标签页"""
    if not 标签页:
        return
    
    try:
        if not 标签页.is_closed():
            标签标题 = await 标签页.title() if 标签页.url else "空白标签页"
            await 标签页.close()
            print(f"🔚 单个标签页已关闭：{标签标题}")
    except Exception as e:
        print(f"❌ 关闭标签页异常：{str(e)}")

async def 关闭浏览器实例(p: any, 浏览器: any, 上下文: any) -> None:
    """异步关闭浏览器所有资源"""
    print(f"\n=====================================")
    print(f"📌 开始关闭浏览器实例...")
    
    try:
        if 上下文:
            await 上下文.close()
            print(f"✅ 浏览器上下文已关闭")
        
        if 浏览器:
            await 浏览器.close()
            print(f"✅ 浏览器实例已关闭")
    except Exception as e:
        print(f"❌ 关闭浏览器异常：{str(e)}")
    finally:
        if p:
            await p.stop()
            print(f"✅ Playwright引擎已停止")
    print(f"🔚 所有浏览器资源释放完毕")