import base64
import json
import requests
import time
import re
from PIL import Image
from io import BytesIO
from playwright.sync_api import sync_playwright
from typing import Tuple

# ===================== 配置项（保留所有核心优化） =====================
智谱API密钥 = "a88b2aade04643109e70ba3bd6182382.m3zspqQJsKTbQx29"
智谱API接口地址 = "https://open.bigmodel.cn/api/paas/v4/chat/completions"
# 模型定义
视觉模型 = "glm-4.5-flash"  # 升级为glm-4.5-flash，思考模式效果更好
# 目标指定网页链接（股吧地址）
目标网页链接 = "https://guba.eastmoney.com/list,000001.html"
# 预设用户提示词（要求AI生成Python代码，输出标准参数）
默认用户提示词 = """
你的任务是生成一段Python代码，用于输出滑块验证码的标准操作参数。
要求如下：
1.  Python代码要求：
   -  仅使用Python内置库（json），无需额外安装依赖。
   -  代码中可包含注释，但最终必须通过`print(json.dumps(参数字典, ensure_ascii=False))`输出标准结果。
   -  无需考虑执行环境，只需要保证代码语法正确，输出结果格式标准。
2.  输出参数字典格式（必须包含trigger和drag两个顶层字段）：
{
    "trigger": {
        "trigger_type": "light_drag",  # 固定值，优先轻微拖动触发缺口
        "trigger_start_x": 整数,        # 滑块起始x坐标（非负，合理范围：100-800）
        "trigger_start_y": 整数,        # 滑块起始y坐标（非负，合理范围：200-700）
        "trigger_end_x": 整数,           # 轻微拖动结束x（=trigger_start_x + 10）
        "trigger_end_y": 整数,           # 轻微拖动结束y（=trigger_start_y）
        "trigger_duration": 0.5          # 固定值，无需修改
    },
    "drag": {
        "drag_start_x": 整数,           # 与trigger_start_x一致
        "drag_start_y": 整数,           # 与trigger_start_y一致
        "drag_end_x": 整数,             # 目标结束x（根据缺口调整，合理范围：trigger_start_x+50 至 800）
        "drag_end_y": 整数,             # 与drag_start_y一致，保持水平拖拽
        "drag_duration": 1.0            # 固定值，模拟真实用户操作
    }
}
3.  额外要求：
   -  若有上一次操作记录，根据上一次的偏差调整本次drag_end_x（偏差=上一次end_x - 缺口x）。
   -  坐标值必须为非负整数，符合网页可视区域范围，避免无效坐标。
   -  代码结构清晰，即使有小偏差，也能保证print输出的JSON格式标准。
4.  禁止输出任何额外内容，仅返回Python代码块（无需代码标记符```python）。
"""
# 验证码自动处理配置
最大验证码重试次数 = 10  # 保留10次容错
验证成功关键词 = ["股吧", "东方财富网", "000001", "帖子"]  # 验证成功的页面标识
验证失败关键词 = ["身份核实", "验证码", "安全验证", "人机验证"]  # 验证未通过的页面标识

# ===================== 整合：浏览器操作工具函数（原打开谷歌网页.py 全部逻辑） =====================
def 创建浏览器实例(
    浏览器类型: str = "chromium",
    无头模式: bool = False
) -> Tuple[any, any, any]:
    """
    同步创建单个浏览器实例、上下文和Playwright实例（同一窗口，会话共享）
    """
    # 启动Playwright实例并保留引用
    p = sync_playwright().start()
    
    # 验证浏览器类型
    支持的浏览器 = ["chromium", "firefox", "webkit"]
    if 浏览器类型 not in 支持的浏览器:
        raise ValueError(f"浏览器类型不合法，可选值：{支持的浏览器}")
    
    # 核心配置：Chrome启动参数，保证同一窗口全屏+稳定+会话共享
    浏览器启动参数 = [
        "--no-sandbox",  # 适配Linux环境
        "--start-fullscreen",  # 启动时直接全屏（Chrome专属）
        "--window-position=0,0",  # 窗口左上角固定在屏幕(0,0)
        "--disable-blink-features=AutomationControlled",  # 隐藏自动化标识，提高稳定性
        "--start-maximized"  # 备用最大化（兼容非Chrome环境）
    ]
    
    # 启动浏览器实例（传入启动参数）
    浏览器 = getattr(p, 浏览器类型).launch(
        headless=无头模式,
        args=浏览器启动参数,
        timeout=60000
    )
    
    # 新建上下文（禁用固定视口+保证同一窗口会话共享）
    上下文 = 浏览器.new_context(
        viewport=None  # 禁用固定视口，适配全屏窗口，保证会话共享
    )
    
    print(f"✅ 浏览器实例创建成功（{浏览器类型}），已全屏打开窗口（同一窗口会话共享）")
    return p, 浏览器, 上下文

def 打开新标签页(
    上下文: any,
    目标链接: str
) -> any:
    """
    在已有浏览器上下文（同一窗口）中，打开新标签页并导航到目标URL（会话共享，避广告）
    """
    try:
        # 新建标签页（同一上下文=同一窗口，会话共享）
        新标签页 = 上下文.new_page()
        
        print(f"\n📌 正在打开新标签页：{目标链接}")
        
        # 导航到目标链接（延长超时，适配验证页面加载，不主动刷新）
        新标签页.goto(
            url=目标链接,
            wait_until="domcontentloaded",
            timeout=60000  # 延长导航超时到60秒
        )
        
        # 打印标签页基本信息
        当前页面标题 = 新标签页.title().strip()
        当前页面URL = 新标签页.url.strip()
        print(f"✅ 新标签页打开成功：{当前页面标题}（URL：{当前页面URL}）")
        print(f"📌 标签页特性：同一窗口会话共享，无额外广告加载（若为第二个标签页）")
        
        return 新标签页
    
    except Exception as e:
        print(f"❌ 新标签页打开失败：{str(e)}")
        # 兜底关闭失败的空白标签页（若存在）
        try:
            新标签页.close()
        except:
            pass
        raise e

def 关闭单个标签页(标签页: any) -> None:
    """
    关闭单个指定标签页，仅释放该标签资源，不影响浏览器上下文和其他标签页
    """
    if not 标签页:
        return
    
    try:
        if not 标签页.is_closed():
            标签标题 = 标签页.title().strip() if 标签页.url else "空白标签页"
            标签页.close()
            print(f"🔚 单个标签页已关闭：{标签标题}")
    except Exception as e:
        print(f"❌ 关闭单个标签页时出现异常：{str(e)}")

def 关闭浏览器实例(p: any, 浏览器: any, 上下文: any) -> None:
    """
    关闭完整的浏览器实例、上下文和Playwright引擎，释放所有相关资源
    """
    print(f"\n=====================================")
    print(f"📌 开始关闭浏览器实例...")
    print(f"=====================================")
    
    try:
        # 第一步：关闭浏览器上下文（先关闭所有标签页）
        if 上下文:
            上下文.close()
            print(f"✅ 浏览器上下文已关闭")
        
        # 第二步：关闭浏览器实例
        if 浏览器:
            浏览器.close()
            print(f"✅ 浏览器实例已关闭")
    
    except Exception as e:
        print(f"❌ 关闭浏览器/上下文时出现异常：{str(e)}")
    
    finally:
        # 第三步：停止Playwright引擎（无论前面是否出错，都必须执行）
        if p:
            p.stop()
            print(f"✅ Playwright引擎已停止")
        print(f"🔚 所有核心资源释放完毕")

# ===================== 核心：浏览器智能体类（滑块优化+AI生成Python代码获取参数） =====================
class 浏览器智能体:
    """基于GLM-4.5-flash的浏览器智能体（整合版：AI生成Python代码+滑块优化）"""
    def __init__(self, 标签页对象: any):
        """
        初始化智能体
        :param 标签页对象: Playwright已打开的Chrome标签页对象（第二个无广告标签页）
        """
        self.标签页 = 标签页对象  # 接收第二个无广告标签页
        self.标签页.set_default_timeout(30000)  # 延长超时到30秒,适配验证加载
        # 初始化上一次操作记录（存储参数、截图、验证结果）
        self.上一次操作记录 = {
            "is_success": False,
            "trigger_params": None,
            "drag_params": None,
            "screenshot_base64": None,
            "error_info": None
        }

    # ===================== 修正：自定义鼠标操作函数（移除delay参数，修复Playwright语法） =====================
    def 鼠标移动到指定位置(self, x: int, y: int, duration: float = 0.5) -> None:
        """
        鼠标从当前位置移动到指定坐标（修复Playwright语法，无delay参数）
        :param x: 目标x坐标
        :param y: 目标y坐标
        :param duration: 移动时长（秒）
        """
        try:
            # Playwright mouse.move() 无delay参数，直接移动，如需延时可拆分步骤
            self.标签页.mouse.move(x, y)
            # 模拟移动时长，短暂延时（可选，模拟真实用户）
            time.sleep(duration / 2)
            print(f"✅ 鼠标已移动到坐标（{x}, {y}），移动时长：{duration}秒")
        except Exception as e:
            raise Exception(f"鼠标移动失败：{str(e)}")

    def 鼠标点击指定位置(self, x: int, y: int, click_type: str = "left") -> None:
        """
        鼠标在指定坐标点击(左键单击默认）
        :param x: 点击x坐标
        :param y: 点击y坐标
        :param click_type: 点击类型（left/right/middle）
        """
        try:
            self.标签页.mouse.click(x, y, button=click_type)
            print(f"✅ 鼠标{click_type}键已点击坐标（{x}, {y}）")
        except Exception as e:
            raise Exception(f"鼠标点击失败：{str(e)}")

    def 鼠标拖拽操作(self, start_x: int, start_y: int, end_x: int, end_y: int, duration: float = 1.0) -> None:
        """
        鼠标拖拽操作（修复Playwright语法，模拟匀速拖拽）
        :param start_x: 起始x坐标
        :param start_y: 起始y坐标
        :param end_x: 结束x坐标
        :param end_y: 结束y坐标
        :param duration: 拖拽时长（秒）
        """
        try:
            # 步骤1：移动到起始位置
            self.鼠标移动到指定位置(start_x, start_y, duration=0.3)
            # 步骤2：按下鼠标左键
            self.标签页.mouse.down()
            print(f"✅ 鼠标已按下左键，起始坐标（{start_x}, {start_y}）")
            # 步骤3：拖拽到结束位置（匀速移动，拆分步骤模拟时长）
            step_count = int(duration * 20)  # 分20步移动，保证匀速效果
            step_x = (end_x - start_x) / step_count
            step_y = (end_y - start_y) / step_count
            step_delay = duration / step_count  # 每步延时
            
            for i in range(step_count):
                current_x = start_x + (step_x * (i + 1))
                current_y = start_y + (step_y * (i + 1))
                self.标签页.mouse.move(current_x, current_y)
                time.sleep(step_delay)  # 每步延时，模拟拖拽时长
            # 步骤4：松开鼠标左键
            self.标签页.mouse.up()
            print(f"✅ 鼠标拖拽完成，结束坐标（{end_x}, {end_y}），拖拽时长：{duration}秒")
        except Exception as e:
            raise Exception(f"鼠标拖拽失败：{str(e)}")

    def 轻微拖动触发缺口(self, start_x: int, start_y: int, drag_offset: int = 10) -> None:
        """
        轻微拖动滑块，触发缺口显现（专用前置操作，封装常用逻辑）
        :param start_x: 滑块起始x坐标
        :param start_y: 滑块起始y坐标
        :param drag_offset: 向右拖动偏移量（默认10px，足够触发缺口）
        """
        try:
            self.鼠标拖拽操作(
                start_x=start_x,
                start_y=start_y,
                end_x=start_x + drag_offset,
                end_y=start_y,
                duration=0.5
            )
            print(f"✅ 轻微拖动触发缺口完成，偏移量：{drag_offset}px")
        except Exception as e:
            raise Exception(f"轻微拖动触发缺口失败：{str(e)}")

    # ===================== 截屏相关函数（拖动结束瞬间截屏+历史截图存储） =====================
    def 捕获页面截图(self, full_page: bool = True) -> str:
        """捕获当前页面截图，返回Base64编码（支持完整页面/可视区域）"""
        try:
            截图二进制数据 = self.标签页.screenshot(
                full_page=full_page,
                type="jpeg",
                quality=80
            )
            截图缓冲流 = BytesIO(截图二进制数据)
            Image.open(截图缓冲流)  # 验证截图有效性
            # 转换为Base64编码返回
            return base64.b64encode(截图缓冲流.getvalue()).decode("utf-8")
        except Exception as e:
            raise Exception(f"页面截屏失败：{str(e)}")

    def 拖动结束瞬间截屏(self) -> str:
        """滑块拖拽结束瞬间截屏，捕捉拖拽后的验证状态（缺口对齐情况）"""
        try:
            # 立即截屏（可视区域，更快捕捉瞬间状态）
            截图_base64 = self.捕获页面截图(full_page=False)
            print(f"✅ 拖动结束瞬间截屏完成，已保存截图Base64编码（可视区域）")
            # 更新上一次操作记录的截图
            self.上一次操作记录["screenshot_base64"] = 截图_base64
            return 截图_base64
        except Exception as e:
            raise Exception(f"拖动结束瞬间截屏失败：{str(e)}")

    # ===================== 核心重构：AI生成Python代码 → 执行代码获取标准参数 =====================
    def 提取_ai_python代码(self, ai_response: str) -> str:
        """提取AI返回中的Python代码（移除多余内容，保留纯代码）"""
        # 正则匹配代码块（兼容有无```python标记）
        code_patterns = [
            r"```python\n(.*?)\n```",  # 匹配带```python标记的代码
            r"```\n(.*?)\n```",        # 匹配带```标记的代码
            r"(.*print\(json\.dumps.*\))"  # 匹配包含核心输出的代码
        ]
        
        for pattern in code_patterns:
            match = re.search(pattern, ai_response, re.DOTALL)
            if match:
                return match.group(1).strip()
        
        # 无匹配时，返回原始响应（去除首尾空白）
        return ai_response.strip()

    def 执行_python代码获取参数(self, python_code: str) -> dict:
        """安全执行AI生成的Python代码，获取标准操作参数"""
        try:
            # 构建局部命名空间（仅允许内置json库，禁止危险操作）
            local_namespace = {
                "json": json,
                "print": print
            }
            
            # 重定向stdout，捕获print输出结果
            import io
            from contextlib import redirect_stdout
            
            stdout_capture = io.StringIO()
            with redirect_stdout(stdout_capture):
                # 执行Python代码（仅在局部命名空间中执行，降低风险）
                exec(python_code, {}, local_namespace)
            
            # 提取捕获的输出结果
            output_result = stdout_capture.getvalue().strip()
            if not output_result:
                raise Exception("AI生成的Python代码未输出任何有效内容")
            
            # 解析JSON参数
            operation_params = json.loads(output_result)
            
            # 验证参数完整性
            if "trigger" not in operation_params or "drag" not in operation_params:
                raise Exception("执行Python代码输出的参数不完整，缺少trigger或drag字段")
            
            print(f"✅ 成功执行Python代码，获取标准操作参数：\n{json.dumps(operation_params, ensure_ascii=False, indent=2)}")
            return operation_params

        except json.JSONDecodeError as e:
            raise Exception(f"Python代码输出结果无法解析为JSON：{str(e)}，输出内容：{output_result}")
        except Exception as e:
            raise Exception(f"执行Python代码失败：{str(e)}，代码内容：{python_code}")

    def 调用智谱API获取操作参数(self) -> dict:
        """调用GLM-4.5-flash（启用思考模式），生成Python代码并执行获取参数"""
        # 构建完整提示词（包含上一次操作记录）
        完整提示词 = f"""
        以下是当前滑块验证码的任务要求：
        {默认用户提示词}
        
        以下是上一次的操作记录（若为第一次尝试，该记录为空）：
        {json.dumps(self.上一次操作记录, ensure_ascii=False, indent=2)}
        
        重要注意事项（必须严格遵守）：
        1.  上一次若验证失败，根据截图判断滑块结束位置与缺口的偏差，调整本次drag_end_x（偏差=滑块结束x - 缺口x）。
        2.  本次拖拽需修正上一次的偏差，页面未刷新，缺口位置保持不变，无需重新识别缺口整体位置。
        3.  前置触发操作固定选择light_drag（偏移10px），确保缺口稳定显现。
        4.  坐标值必须为非负整数，符合网页可视区域范围（x：100-800，y：200-700）。
        5.  Python代码必须包含`print(json.dumps(参数字典, ensure_ascii=False))`，确保输出标准JSON。
        6.  无需额外注释，代码语法必须正确，可直接执行。
        """

        # 构建请求消息列表（包含当前截图+上一次截图（若有））
        消息列表 = [
            {
                "role": "system",
                "content": "你是滑块验证码Python代码生成专家，仅返回符合格式要求的Python代码，代码可直接执行并输出标准JSON参数。"
            },
            {
                "role": "user",
                "content": [
                    # 当前页面截图（必传）
                    {
                        "type": "image_url",
                        "image_url": {
                            "url": f"data:image/jpeg;base64,{self.捕获页面截图()}"
                        }
                    },
                    # 上一次操作截图（可选，存在则传）
                    *(
                        [
                            {
                                "type": "image_url",
                                "image_url": {
                                    "url": f"data:image/jpeg;base64,{self.上一次操作记录['screenshot_base64']}"
                                }
                            }
                        ]
                        if self.上一次操作记录["screenshot_base64"]
                        else []
                    ),
                    # 文本提示词（必传）
                    {
                        "type": "text",
                        "text": 完整提示词
                    }
                ]
            }
        ]

        try:
            # 构建请求头和请求体（启用思考模式，提高代码生成准确性）
            请求头 = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {智谱API密钥}"
            }
            请求体 = {
                "model": 视觉模型,
                "messages": 消息列表,
                "temperature": 0.3,  # 降低随机性，保证参数稳定性
                "max_tokens": 2048,
                "thinking": True  # 启用思考模式，不混淆输出结果，提高代码质量
            }

            # 发送API请求
            响应结果 = requests.post(
                url=智谱API接口地址,
                headers=请求头,
                json=请求体,
                timeout=60
            )
            响应结果.raise_for_status()

            # 解析响应结果
            结果数据 = 响应结果.json()
            ai_response = 结果数据["choices"][0]["message"]["content"].strip()

            # 步骤1：提取Python代码
            python_code = self.提取_ai_python代码(ai_response)
            print(f"✅ 成功提取AI生成的Python代码：\n{python_code}")

            # 步骤2：执行Python代码，获取标准参数
            operation_params = self.执行_python代码获取参数(python_code)

            return operation_params

        except requests.exceptions.HTTPError as e:
            if 响应结果.status_code == 429:
                raise Exception(f"智谱API调用失败：429 请求过于频繁（超出配额/限流），请稍后重试或检查API配额") from e
            else:
                raise Exception(f"智谱API调用失败：HTTP错误 {响应结果.status_code} - {响应结果.reason}") from e
        except Exception as e:
            raise Exception(f"智谱API调用/代码执行失败：{str(e)}") from e

    # ===================== 验证码判断逻辑（不主动刷新，等待页面自动刷新） =====================
    def 判断是否需要验证码验证(self) -> bool:
        """判断是否需要验证码验证（基于当前页面状态，不主动刷新）"""
        print("\n🔍 正在判断是否需要验证码验证...")
        页面标题 = self.标签页.title().strip()
        页面URL = self.标签页.url.strip()

        # 核心判断逻辑：标题包含验证失败关键词，且为东方财富网页面
        需要验证 = (
            any(关键词 in 页面标题 for 关键词 in 验证失败关键词)
            and "eastmoney.com" in 页面URL
        )

        # 打印判断详情
        print(f"📊 页面详情 - 标题：{页面标题} | URL：{页面URL}")
        print(f"📊 最终判断结果：{'需要继续处理滑块验证码' if 需要验证 else '无需验证码验证'}")
        return 需要验证

    def 判断验证码是否验证成功(self) -> bool:
        """判断验证码是否验证成功（等待页面自动刷新，不主动执行刷新操作）"""
        print("\n🔍 正在判断验证码是否验证成功（等待页面自动刷新）...")
        等待超时时间 = 15  # 等待页面自动刷新的超时时间（秒）
        开始时间 = time.time()

        while time.time() - 开始时间 < 等待超时时间:
            页面标题 = self.标签页.title().strip()
            页面URL = self.标签页.url.strip()

            # 验证成功条件：页面标题包含成功关键词（说明已自动刷新）
            验证成功 = any(关键词 in 页面标题 for 关键词 in 验证成功关键词)

            if 验证成功:
                print(f"✅ 验证码验证成功！页面已自动刷新，新标题：{页面标题}")
                self.上一次操作记录["is_success"] = True
                return True
            else:
                # 等待1秒后再次判断，避免频繁查询
                已等待时长 = int(time.time() - 开始时间)
                print(f"⌛ 等待页面自动刷新中...（已等待{已等待时长}秒 / 总超时{等待超时时间}秒）")
                time.sleep(1)

        # 超时未自动刷新，判定为验证失败
        print(f"❌ 等待页面自动刷新超时（{等待超时时间}秒），本次滑块验证失败")
        self.上一次操作记录["is_success"] = False
        return False

    # ===================== 核心：滑块验证流程（不刷新+前置触发+历史参考+10次重试） =====================
    def 处理滑块验证码(self) -> bool:
        """处理滑块验证码（不刷新页面+前置触发缺口+历史操作参考+10次重试）"""
        重试次数 = 0

        while 重试次数 < 最大验证码重试次数:
            try:
                print(f"\n=====================================")
                print(f"📌 第{重试次数+1}次尝试处理滑块验证码（共{最大验证码重试次数}次）")
                print(f"=====================================")

                # 步骤1：判断当前页面是否需要继续处理验证码
                if not self.判断是否需要验证码验证():
                    print("✅ 当前页面无需继续处理验证码，直接返回成功")
                    return True

                # 步骤2：调用AI生成Python代码，执行获取本次操作参数
                print("\n📝 正在调用智谱API生成Python代码，获取滑块操作参数...")
                操作参数 = self.调用智谱API获取操作参数()
                trigger_params = 操作参数["trigger"]
                drag_params = 操作参数["drag"]

                # 步骤3：执行前置触发操作，确保缺口显现
                print("\n📝 正在执行前置触发操作，触发缺口显现...")
                if trigger_params.get("trigger_type") == "click":
                    # 点击触发缺口
                    self.鼠标点击指定位置(
                        x=trigger_params["trigger_start_x"],
                        y=trigger_params["trigger_start_y"]
                    )
                else:
                    # 轻微拖动触发缺口（默认优先选择）
                    self.鼠标拖拽操作(
                        start_x=trigger_params["trigger_start_x"],
                        start_y=trigger_params["trigger_start_y"],
                        end_x=trigger_params["trigger_end_x"],
                        end_y=trigger_params["trigger_end_y"],
                        duration=trigger_params["trigger_duration"]
                    )

                # 步骤4：等待缺口完全渲染（短暂等待，保证稳定性）
                time.sleep(1)
                print("✅ 缺口已显现，准备执行正式滑块拖拽操作...")

                # 步骤5：执行正式滑块拖拽操作
                print("\n📝 正在执行正式滑块拖拽操作...")
                self.鼠标拖拽操作(
                    start_x=drag_params["drag_start_x"],
                    start_y=drag_params["drag_start_y"],
                    end_x=drag_params["drag_end_x"],
                    end_y=drag_params["drag_end_y"],
                    duration=drag_params["drag_duration"]
                )

                # 步骤6：拖动结束瞬间截屏，保存本次操作状态
                self.拖动结束瞬间截屏()

                # 步骤7：更新上一次操作记录（保存本次参数，便于下一次参考）
                self.上一次操作记录.update({
                    "trigger_params": trigger_params,
                    "drag_params": drag_params,
                    "error_info": None,
                    "is_success": False
                })

                # 步骤8：判断本次验证是否成功（等待页面自动刷新）
                if self.判断验证码是否验证成功():
                    return True

                # 步骤9：验证失败，准备下一次重试（不刷新页面，保留缺口状态）
                重试次数 += 1
                if 重试次数 < 最大验证码重试次数:
                    剩余次数 = 最大验证码重试次数 - 重试次数
                    print(f"❌ 第{重试次数}次验证失败，不刷新页面，5秒后进行下一次尝试（剩余{剩余次数}次）")
                    time.sleep(5)  # 等待5秒，保留页面状态，缺口位置不变
                else:
                    print(f"❌ 已达到最大重试次数（{最大验证码重试次数}次），滑块验证码处理失败")

            except Exception as e:
                重试次数 += 1
                错误信息 = str(e)
                self.上一次操作记录["error_info"] = 错误信息
                print(f"❌ 第{重试次数}次处理滑块验证码异常：{错误信息}")

                if 重试次数 < 最大验证码重试次数:
                    剩余次数 = 最大验证码重试次数 - 重试次数
                    print(f"⌛ 不刷新页面，5秒后进行下一次尝试（剩余{剩余次数}次）")
                    time.sleep(5)
                else:
                    print(f"❌ 已达到最大重试次数，滑块验证码处理失败")

        return False

    def 运行智能体全自动流程(self) -> bool:
        """运行智能体全自动流程（整合：双标签页避广告+滑块优化）"""
        print("="*60)
        print("开始执行智能体全自动流程（整合版：AI生成Python代码+滑块优化）")
        print("="*60)

        try:
            # 核心步骤：处理滑块验证码（不刷新+历史参考+10次重试）
            验证码处理结果 = self.处理滑块验证码()

            if not 验证码处理结果:
                raise Exception("滑块验证码处理失败，无法完成本次任务")

            print("\n✅ 智能体全自动流程执行完成，任务成功！")
            return True

        except Exception as e:
            print(f"\n❌ 智能体全自动流程执行失败：{str(e)}")
            return False

# ===================== 主函数：整合所有流程，直接运行 =====================
def 运行股吧全自动智能体() -> bool:
    """主函数：同一窗口双标签页避广告+滑块优化处理+资源自动清理"""
    操作结果 = False
    p = None
    浏览器 = None
    上下文 = None
    第一个广告标签页 = None
    第二个操作标签页 = None

    try:
        # 步骤1：创建浏览器实例（同一窗口，会话共享）
        print("\n=====================================")
        print("📌 步骤1：创建浏览器实例（同一窗口，会话共享）")
        print("=====================================")
        p, 浏览器, 上下文 = 创建浏览器实例(浏览器类型="chromium", 无头模式=False)

        # 步骤2：打开第一个标签页（承接所有广告，无需操作）
        print("\n=====================================")
        print("📌 步骤2：打开第一个标签页（承接广告）")
        print("=====================================")
        第一个广告标签页 = 打开新标签页(上下文, 目标网页链接)
        time.sleep(5)  # 等待5秒，确保广告完全加载并被承接

        # 步骤3：打开第二个标签页（无广告，用于后续滑块操作）
        print("\n=====================================")
        print("📌 步骤3：打开第二个标签页（无广告，用于操作）")
        print("=====================================")
        第二个操作标签页 = 打开新标签页(上下文, 目标网页链接)

        # 步骤4：关闭第一个广告标签页（清理冗余资源，避免干扰）
        print("\n=====================================")
        print("📌 步骤4：关闭第一个广告标签页（清理冗余）")
        print("=====================================")
        关闭单个标签页(第一个广告标签页)

        # 步骤5：初始化浏览器智能体，执行核心流程
        print("\n=====================================")
        print("📌 步骤5：初始化智能体，执行滑块验证码处理")
        print("=====================================")
        股吧智能体 = 浏览器智能体(第二个操作标签页)
        操作结果 = 股吧智能体.运行智能体全自动流程()

        # 步骤6：关闭所有浏览器资源（自动清理）
        print("\n=====================================")
        print("📌 步骤6：关闭浏览器所有资源")
        print("=====================================")
        关闭浏览器实例(p, 浏览器, 上下文)

        return 操作结果

    except Exception as e:
        print(f"\n❌ 主流程执行异常：{str(e)}")
        # 兜底清理所有资源，避免内存泄漏
        try:
            if 第一个广告标签页:
                关闭单个标签页(第一个广告标签页)
            if 第二个操作标签页:
                关闭单个标签页(第二个操作标签页)
            if p and 浏览器 and 上下文:
                关闭浏览器实例(p, 浏览器, 上下文)
        except:
            pass
        return False

# ===================== 直接运行入口（无需依赖其他文件，直接执行） =====================
if __name__ == "__main__":
    最终操作结果 = 运行股吧全自动智能体()
    print(f"\n=====================================")
    print(f"📊 本次任务最终结果：{'✅ 成功' if 最终操作结果 else '❌ 失败'}")
    print(f"=====================================")