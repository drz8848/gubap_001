from playwright.sync_api import sync_playwright
from typing import Tuple
import time

def 创建浏览器实例(
    浏览器类型: str = "chromium",
    无头模式: bool = False
) -> Tuple[any, any, any]:
    """
    同步创建单个浏览器实例、上下文和Playwright实例（用于后续关闭引擎）
    新增：全屏配置，固定窗口位置，避免标签页偏移
    """
    # 启动Playwright实例并保留引用
    p = sync_playwright().start()
    
    # 验证浏览器类型
    支持的浏览器 = ["chromium", "firefox", "webkit"]
    if 浏览器类型 not in 支持的浏览器:
        raise ValueError(f"浏览器类型不合法，可选值：{支持的浏览器}")
    
    # 核心配置：Chrome启动参数，直接全屏/最大化（优先全屏）
    浏览器启动参数 = [
        "--no-sandbox",  # 适配Linux环境（原有保留）
        "--start-fullscreen",  # 关键参数1：启动时直接全屏（Chrome专属）
        "--window-position=0,0",  # 关键参数2：窗口左上角固定在屏幕(0,0)坐标
        "--disable-popup-positioning",  # 关键参数3：禁用弹窗/新标签页自动偏移定位
        "--start-maximized"  # 关键参数4：最大化窗口（备用，若--start-fullscreen不生效）
    ]
    
    # 启动浏览器实例（传入新增的全屏/固定位置参数）
    浏览器 = getattr(p, 浏览器类型).launch(
        headless=无头模式,
        args=浏览器启动参数
    )
    
    # 创建上下文后，强制设置窗口最大化（兜底，确保全屏状态稳定）
    上下文 = 浏览器.new_context()
    
    print(f"✅ 浏览器实例创建成功（{浏览器类型}），已全屏打开窗口，等待创建标签页")
    return p, 浏览器, 上下文

def 打开新标签页(
    上下文: any,
    目标链接: str
) -> any:
    """
    在已有浏览器上下文（同一窗口）中，打开新标签页并导航到目标URL
    新增：通用身份核实检测，触发验证时保持页面打开，等待用户手动输入验证码
    """
    try:
        新标签页 = 上下文.new_page()
        
        print(f"\n📌 正在打开新标签页：{目标链接}")
        新标签页.goto(
            url=目标链接,
            wait_until="domcontentloaded",
            timeout=60000  # 延长基础超时到60秒，给验证留时间
        )
        
        # ========== 新增：通用身份核实处理逻辑（核心修改） ==========
        验证关键词 = ["身份核实", "验证", "安全验证", "人机验证"]
        最大等待次数 = 5  # 最多等待5次用户验证（足够手动操作）
        验证等待计数 = 0
        
        while 验证等待计数 < 最大等待次数:
            # 获取当前页面标题和URL，判断是否触发身份核实
            当前页面标题 = 新标签页.title().strip()
            当前页面URL = 新标签页.url.strip()
            
            # 检测是否触发身份核实（标题包含验证关键词，且是东方财富网域名）
            触发验证 = any(关键词 in 当前页面标题 for 关键词 in 验证关键词) and \
                        "eastmoney.com" in 当前页面URL
            
            if 触发验证:
                print(f"\n⚠️  触发通用身份核实（链接：{目标链接[:50]}...）")
                print(f"✅ 页面已保持打开，请在浏览器中手动完成验证码输入/身份验证")
                print(f"📌 验证完成后，请回到终端按【回车】继续...")
                
                # 阻塞等待用户确认（保持页面打开，直到用户回车）
                input()
                print(f"📌 收到用户确认，等待页面加载完成...")
                time.sleep(3)  # 给页面3秒跳转/加载时间
                验证等待计数 += 1
                
                # 重新刷新页面（可选，防止验证后页面未加载完成）
                try:
                    新标签页.reload(wait_until="domcontentloaded", timeout=60000)
                except:
                    pass
            else:
                # 未触发验证，跳出循环，继续后续流程
                break
        
        # 验证超时判断
        if 验证等待计数 >= 最大等待次数:
            print(f"❌ 身份核实超时（链接：{目标链接[:50]}...），无法继续打开标签页")
            新标签页.close()
            return None
        
        # ========== 原有逻辑保留 ==========
        print(f"✅ 新标签页打开成功：{新标签页.title()}（URL：{新标签页.url}）")
        return 新标签页
    except Exception as e:
        print(f"❌ 新标签页打开失败：{str(e)}")
        # 兜底关闭失败的空白标签页
        try:
            新标签页.close()
        except:
            pass
        raise e

def 关闭单个标签页(标签页: any) -> None:
    """
    关闭单个指定标签页，仅释放该标签资源，不影响浏览器和上下文
    """
    if not 标签页:
        return
    
    try:
        if not 标签页.is_closed():
            标签标题 = 标签页.title() if 标签页.url else "空白标签页"
            标签页.close()
            print(f"🔚 单个标签页已关闭：{标签标题}")
    except Exception as e:
        print(f"❌ 关闭单个标签页时出现异常：{str(e)}")

def 关闭浏览器实例(p: any, 浏览器: any, 上下文: any) -> None:
    """
    关闭浏览器实例、上下文和Playwright引擎，释放所有资源
    """
    print(f"\n=====================================")
    print(f"📌 开始关闭浏览器实例...")
    
    try:
        # 关闭上下文（对应浏览器窗口）
        if 上下文:
            上下文.close()
            print(f"✅ 浏览器上下文已关闭")
        
        # 关闭浏览器实例
        if 浏览器:
            浏览器.close()
            print(f"✅ 浏览器实例已关闭")
    except Exception as e:
        print(f"❌ 关闭浏览器实例过程中出现异常：{str(e)}")
    finally:
        # 停止Playwright引擎（使用保留的实例p）
        if p:
            p.stop()
            print(f"✅ Playwright引擎已停止")
        print(f"🔚 所有核心资源释放完毕")