from playwright.sync_api import sync_playwright
import time

def open_chrome_manually_for_verification():
    with sync_playwright() as p:
        # 启动谷歌浏览器（Chrome），保留你的核心配置
        browser = p.chromium.launch(
            channel="chrome",  # 指定使用Chrome浏览器（需提前安装Chrome，且环境变量可访问）
            headless=False,    # 禁用无头模式，显示浏览器窗口
            slow_mo=500,       # 操作延迟500ms，方便观察页面加载
            args=[
                "--start-maximized",  # 浏览器全屏打开
                "--no-sandbox",       # 解决部分Linux/服务器环境下的权限问题
                "--disable-dev-shm-usage"  # 优化内存占用，避免容器/低配环境报错
            ]
        )
        
        # 🌟 核心修复：移除初始化时的storage_state参数（文件尚未创建，无法读取）
        # 先创建干净的浏览器上下文，后续操作完成后再保存状态
        context = browser.new_context(
            viewport=None,  # 配合--start-maximized，使用浏览器全屏视口，不限制固定尺寸
        )
        
        # 创建新标签页
        page = context.new_page()
        
        try:
            # 访问东方财富股吧目标URL
            target_url = "https://guba.eastmoney.com/list,000001_1.html"
            print(f"📌 正在打开目标页面：{target_url}")
            
            # 等待页面加载完成（networkidle：网络请求全部完成后停止等待，适合手动操作）
            page.goto(target_url, wait_until="networkidle")
            print("\n" + "="*60)
            print(f"✅ 页面加载完成，进入手动操作阶段！")
            print(f"1. 请在当前Chrome窗口中完成身份核实/验证码验证")
            print(f"2. 操作完成后，返回控制台按下【回车】键保存身份状态")
            print(f"3. 请勿提前关闭Chrome浏览器窗口，否则状态无法保存")
            print("="*60)
            
            # 阻塞脚本运行，保持浏览器打开，供手动操作
            input()  # 按下回车前，浏览器持续保持打开状态
            
            # 🌟 操作完成后，保存身份状态到文件（此时才创建guba_auth_state.json）
            context.storage_state(path="guba_auth_state.json")
            print(f"\n✅ 身份状态已成功保存到：guba_auth_state.json")
            print(f"💡 后续爬取脚本可加载该文件，直接复用已验证的身份状态，无需重复手动操作")
            
        except KeyboardInterrupt:
            print("\n📌 用户主动中断操作，未保存身份状态")
        except Exception as e:
            print(f"\n❌ 运行过程中出现异常：{str(e)}")
        finally:
            # 🌟 保留浏览器打开状态（注释关闭逻辑，由你手动关闭浏览器窗口）
            # 若需要自动关闭，可取消注释以下两行
            # context.close()
            # browser.close()
            print("\n✅ 脚本逻辑执行完毕，Chrome浏览器将保持打开状态，你可手动关闭或继续操作")

if __name__ == "__main__":
    open_chrome_manually_for_verification()