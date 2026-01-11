import tkinter as tk
from tkinter import scrolledtext, messagebox, ttk
import ttkbootstrap as ttkb
from ttkbootstrap.scrolled import ScrolledText
import textwrap
import sys
import io
import datetime
import traceback
import os

# ========== WSL 环境中文适配（关键！设置字符编码和字体环境变量） ==========
os.environ["LC_ALL"] = "zh_CN.UTF-8"
os.environ["LANG"] = "zh_CN.UTF-8"
# 强制 stdout/stderr 编码为 UTF-8
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')
if hasattr(sys.stderr, 'reconfigure'):
    sys.stderr.reconfigure(encoding='utf-8')

# ========== 核心组件定义（对应你的现有功能模块） ==========
COMPONENTS = [
    {
        "name": "初始化数据库连接",
        "desc": "建立数据库连接，全程复用",
        "params": [],
        "code": """
# 初始化数据库连接（全程复用，直到程序结束/报错关闭）
数据库连接, 数据库游标 = 连接数据库.建立数据库连接()
if not (数据库连接 and 数据库游标):
    print("❌ 数据库连接初始化失败，程序终止")
    exit(1)
"""
    },
    {
        "name": "获取爬取任务列表",
        "desc": "从数据库读取/补充爬取任务",
        "params": [],
        "code": """
# 步骤1：获取任务列表（DataFrame格式）
任务数据框 = 设置任务状态并获取任务列表.初始化并读取爬取任务列表(True)
print(f"\\n📊 任务列表加载完成，共{len(任务数据框)}条任务")
print(任务数据框.head())
"""
    },
    {
        "name": "创建浏览器实例",
        "desc": "初始化Playwright浏览器，复用减少开销",
        "params": [
            {"name": "无头模式", "type": "bool", "default": False}
        ],
        "code": """
# 步骤2：初始化浏览器实例（全程复用，减少资源开销）
p, 浏览器, 上下文 = None, None, None
保持标签页 = None
try:
    p, 浏览器, 上下文 = 打开谷歌网页.创建浏览器实例(无头模式={无头模式})
    # 打开保持标签页（防止浏览器自动关闭）
    保持标签页 = 打开谷歌网页.打开新标签页(上下文, "https://guba.eastmoney.com/list,000001_1.html")
"""
    },
    {
        "name": "遍历股票任务",
        "desc": "遍历任务列表中的股票代码，爬取单只股票",
        "params": [
            {"name": "最大爬取股票数", "type": "int", "default": 2}
        ],
        "code": """
# 步骤3：遍历任务列表中的股票代码
控制变量1 = 0  # 股票数量控制
目标列 = "stock_code"
for 股票代码 in 任务数据框[目标列]:
    控制变量1 += 1
    第一层网页链接 = f"https://guba.eastmoney.com/list,{股票代码}_1.html"
    
    try:
        # 3.1 打开股票股吧首页，获取总页数
        主标签页 = 打开谷歌网页.打开新标签页(上下文, 第一层网页链接)
        总页数 = 获取股票文章页数.获取股吧总页数(主标签页)
        if not 总页数:
            print(f"⚠️  无法获取{股票代码}总页数，跳过该股票")
            打开谷歌网页.关闭单个标签页(主标签页)
            continue
        print(f"📌 股票{股票代码}总页数：{总页数}，开始从后往前爬取")
        
        # 3.2 从后往前遍历页码（总页数→1）
        控制变量2 = 0  # 每页数量控制
        {遍历页码代码}
        
        # 关闭主标签页
        打开谷歌网页.关闭单个标签页(主标签页)
        
    except Exception as e:
        print(f"\\n❌ 第一层标签页（股票首页）运行失败：{str(e)}")
    
    # 调试：限制爬取股票数量
    if 控制变量1 == {最大爬取股票数}:
        break
"""
    },
    {
        "name": "遍历股票页码",
        "desc": "从后往前遍历单只股票的所有页码",
        "params": [
            {"name": "最大爬取页数", "type": "int", "default": 2}
        ],
        "code": """
for 页码 in range(总页数, 0, -1):
    控制变量2 += 1
    try:
        # 打开当前页码的股吧列表页
        第二层网页链接 = f"https://guba.eastmoney.com/list,{股票代码}_{页码}.html"
        分页标签页 = 打开谷歌网页.打开新标签页(上下文, 第二层网页链接)
        
        # 提取当前页文章列表
        文章数据框 = 提取文章列表.提取股吧文章列表(分页标签页, 股票代码, 页码)
        if 文章数据框.empty:
            打开谷歌网页.关闭单个标签页(分页标签页)
            continue
        
        # 3.3 遍历当前页的每篇文章，获取详情并写入数据库
        控制变量3 = 0  # 单页文章数量控制
        {遍历文章代码}
        
        # 更新当前股票的已爬页数
        更新爬取页数.更新任务已爬页数(数据库连接, 数据库游标, 股票代码, 总页数, 页码)
        
        # 关闭分页标签页
        打开谷歌网页.关闭单个标签页(分页标签页)
        
    except Exception as e:
        print(f"\\n❌ 第二层标签页（分页列表）运行失败：{str(e)}")
    
    # 调试：限制爬取页数
    if 控制变量2 == {最大爬取页数}:
        break
"""
    },
    {
        "name": "遍历页面文章",
        "desc": "遍历单页的所有文章，获取详情并写入数据库",
        "params": [
            {"name": "最大爬取文章数", "type": "int", "default": 90}
        ],
        "code": """
详情页URL列 = "详情页URL"
for 详情页链接 in 文章数据框[详情页URL列]:
    控制变量3 += 1
    try:
        # 打开文章详情页
        最后标签页 = 打开谷歌网页.打开新标签页(上下文, 详情页链接)
        
        # 获取点赞数和发布时间
        点赞数, 发布时间 = 获取点赞和完整文章发布时间.获取文章点赞和发布时间(最后标签页, 详情页链接)
        
        # 构造帖子数据字典（匹配数据库表结构）
        文章行 = 文章数据框[文章数据框[详情页URL列] == 详情页链接].iloc[0]
        帖子数据 = {
            "stock_code": 股票代码,
            "post_title": 文章行["标题"],
            "post_url": 详情页链接,
            "post_read_count": str(文章行["阅读"]),
            "post_comment_count": str(文章行["评论"]),
            "like_num": 点赞数,
            "author_name": 文章行["作者"],
            "post_update_time": 文章行["最后更新"],
            "post_create_time": 发布时间,
            "crawl_time": datetime.datetime.now()
        }
        
        # 写入/更新数据库
        写入股吧帖子数据.写入单篇帖子数据(数据库连接, 数据库游标, 帖子数据)
        
        # 关闭文章详情页标签页
        打开谷歌网页.关闭单个标签页(最后标签页)
        
    except Exception as e:
        print(f"\\n❌ 第三层标签页（文章详情）运行失败：{str(e)}")
    
    # 调试：限制单页文章数量
    if 控制变量3 == {最大爬取文章数}:
        break
"""
    },
    {
        "name": "资源释放",
        "desc": "关闭浏览器、数据库连接，释放所有资源",
        "params": [],
        "code": """
# 延迟1秒，确保所有操作完成
time.sleep(1)

except Exception as e:
    print(f"\\n❌ 浏览器相关操作运行失败：{str(e)}")
finally:
    # 关闭保持标签页和浏览器实例
    if 保持标签页:
        打开谷歌网页.关闭单个标签页(保持标签页)
    if all([p, 浏览器, 上下文]):
        打开谷歌网页.关闭浏览器实例(p, 浏览器, 上下文)

except Exception as e:
    print(f"\\n❌ 整体程序运行失败：{str(e)}")
finally:
    # 兜底关闭数据库连接（无论程序成败，都释放资源）
    连接数据库.关闭数据库连接(数据库连接, 数据库游标)
    print("\\n🎉 程序运行结束，所有资源已释放")
"""
    }
]

# ========== 可视化应用主类 ==========
class CrawlerVisualEditor:
    def __init__(self, root):
        self.root = root
        self.root.title("东方财富股吧爬虫可视化编辑器")
        self.root.geometry("1200x800")
        
        # ========== WSL 专属：全局样式配置（思源黑体，所有ttk组件继承） ==========
        style = ttkb.Style()
        # 1. 全局基础样式（所有ttk组件默认字体）
        style.configure(".", 
                        font=("Noto Sans CJK SC", 10),  # 思源黑体（WSL已安装）
                        foreground="#333333",
                        background="#f8f9fa")
        # 2. 加粗样式（用于标题）
        style.configure("Bold.TLabel", 
                        font=("Noto Sans CJK SC", 12, "bold"))
        # ========== 样式配置结束 ==========
        
        # 初始化变量
        self.selected_components = []  # 选中的组件列表
        self.component_params = {}     # 组件参数配置
        self.control_vars = {
            "max_stocks": tk.IntVar(value=2),
            "max_pages": tk.IntVar(value=2),
            "max_articles": tk.IntVar(value=90),
            "headless": tk.BooleanVar(value=False)
        }
        
        # 创建界面
        self._create_widgets()
        
    def _create_widgets(self):
        # 主容器
        main_paned = ttkb.PanedWindow(self.root, orient=tk.HORIZONTAL)
        main_paned.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # 左侧面板：配置 + 组件选择
        left_frame = ttkb.Frame(main_paned, width=300)
        main_paned.add(left_frame, weight=1)
        
        # 1. 配置面板（用Bold.TLabel样式实现加粗）
        config_label = ttkb.Label(left_frame, text="⚙️ 基础配置", style="Bold.TLabel")
        config_label.pack(anchor=tk.W, padx=5, pady=5)
        
        # 控制变量配置
        control_frame = ttkb.LabelFrame(left_frame, text="爬取控制", padding=5)
        control_frame.pack(fill=tk.X, padx=5, pady=3)
        
        # ttk组件不再传font参数，继承全局样式
        ttkb.Label(control_frame, text="最大爬取股票数：").grid(row=0, column=0, sticky=tk.W, padx=2, pady=2)
        ttkb.Entry(control_frame, textvariable=self.control_vars["max_stocks"], width=10).grid(row=0, column=1, padx=2, pady=2)
        
        ttkb.Label(control_frame, text="单股票最大页数：").grid(row=1, column=0, sticky=tk.W, padx=2, pady=2)
        ttkb.Entry(control_frame, textvariable=self.control_vars["max_pages"], width=10).grid(row=1, column=1, padx=2, pady=2)
        
        ttkb.Label(control_frame, text="单页最大文章数：").grid(row=2, column=0, sticky=tk.W, padx=2, pady=2)
        ttkb.Entry(control_frame, textvariable=self.control_vars["max_articles"], width=10).grid(row=2, column=1, padx=2, pady=2)
        
        # Checkbutton也不再传font参数
        ttkb.Checkbutton(control_frame, text="浏览器无头模式", variable=self.control_vars["headless"]).grid(row=3, column=0, columnspan=2, sticky=tk.W, padx=2, pady=2)
        
        # 2. 组件选择面板
        component_label = ttkb.Label(left_frame, text="🧩 功能组件", style="Bold.TLabel")
        component_label.pack(anchor=tk.W, padx=5, pady=5)
        
        # Listbox是tk原生组件，支持直接传font
        self.component_listbox = tk.Listbox(left_frame, height=10, font=("Noto Sans CJK SC", 10))
        self.component_listbox.pack(fill=tk.BOTH, expand=True, padx=5, pady=3)
        for comp in COMPONENTS:
            self.component_listbox.insert(tk.END, comp["name"])
        
        # 组件操作按钮（ttk组件，继承全局字体）
        btn_frame = ttkb.Frame(left_frame)
        btn_frame.pack(fill=tk.X, padx=5, pady=3)
        
        ttkb.Button(btn_frame, text="添加组件", command=self._add_component, bootstyle="success").pack(side=tk.LEFT, padx=2)
        ttkb.Button(btn_frame, text="移除组件", command=self._remove_component, bootstyle="danger").pack(side=tk.LEFT, padx=2)
        ttkb.Button(btn_frame, text="上移", command=self._move_up).pack(side=tk.LEFT, padx=2)
        ttkb.Button(btn_frame, text="下移", command=self._move_down).pack(side=tk.LEFT, padx=2)
        
        # 已选组件列表标题
        selected_label = ttkb.Label(left_frame, text="📋 已选组件（执行顺序）", style="Bold.TLabel")
        selected_label.pack(anchor=tk.W, padx=5, pady=5)
        
        # Listbox是tk原生组件，传font参数
        self.selected_listbox = tk.Listbox(left_frame, height=8, font=("Noto Sans CJK SC", 10))
        self.selected_listbox.pack(fill=tk.BOTH, expand=True, padx=5, pady=3)
        
        # 中间面板：代码预览
        middle_frame = ttkb.Frame(main_paned, width=400)
        main_paned.add(middle_frame, weight=2)
        
        code_label = ttkb.Label(middle_frame, text="📝 生成的主程序代码", style="Bold.TLabel")
        code_label.pack(anchor=tk.W, padx=5, pady=5)
        
        # ScrolledText是tk原生组件，混合字体（等宽+思源黑体）
        self.code_text = ScrolledText(middle_frame, 
                                     font=("DejaVu Sans Mono, Noto Sans CJK SC", 10), 
                                     wrap=tk.NONE)
        self.code_text.pack(fill=tk.BOTH, expand=True, padx=5, pady=3)
        
        # 代码操作按钮
        code_btn_frame = ttkb.Frame(middle_frame)
        code_btn_frame.pack(fill=tk.X, padx=5, pady=3)
        
        ttkb.Button(code_btn_frame, text="生成代码", command=self._generate_code, bootstyle="primary").pack(side=tk.LEFT, padx=2)
        ttkb.Button(code_btn_frame, text="复制代码", command=self._copy_code, bootstyle="info").pack(side=tk.LEFT, padx=2)
        ttkb.Button(code_btn_frame, text="保存代码", command=self._save_code, bootstyle="warning").pack(side=tk.LEFT, padx=2)
        
        # 右侧面板：运行日志
        right_frame = ttkb.Frame(main_paned, width=300)
        main_paned.add(right_frame, weight=1)
        
        log_label = ttkb.Label(right_frame, text="📜 运行日志", style="Bold.TLabel")
        log_label.pack(anchor=tk.W, padx=5, pady=5)
        
        # 日志面板：混合字体
        self.log_text = ScrolledText(right_frame, 
                                     font=("DejaVu Sans Mono, Noto Sans CJK SC", 10), 
                                     wrap=tk.WORD)
        self.log_text.pack(fill=tk.BOTH, expand=True, padx=5, pady=3)
        
        # 运行按钮
        run_btn_frame = ttkb.Frame(right_frame)
        run_btn_frame.pack(fill=tk.X, padx=5, pady=3)
        
        ttkb.Button(run_btn_frame, text="运行爬虫", command=self._run_crawler, bootstyle="success outline").pack(side=tk.LEFT, padx=2)
        ttkb.Button(run_btn_frame, text="清空日志", command=self._clear_log, bootstyle="danger outline").pack(side=tk.LEFT, padx=2)
    
    # ========== 组件操作方法 ==========
    def _add_component(self):
        """添加选中的组件到执行列表"""
        selected_idx = self.component_listbox.curselection()
        if not selected_idx:
            messagebox.showwarning("提示", "请先选择一个组件！")
            return
        
        comp_idx = selected_idx[0]
        comp = COMPONENTS[comp_idx]
        self.selected_components.append(comp)
        self.selected_listbox.insert(tk.END, comp["name"])
        self._generate_code()  # 自动更新代码
    
    def _remove_component(self):
        """移除选中的组件"""
        selected_idx = self.selected_listbox.curselection()
        if not selected_idx:
            messagebox.showwarning("提示", "请先选择要移除的组件！")
            return
        
        idx = selected_idx[0]
        del self.selected_components[idx]
        self.selected_listbox.delete(idx)
        self._generate_code()
    
    def _move_up(self):
        """上移组件"""
        selected_idx = self.selected_listbox.curselection()
        if not selected_idx or selected_idx[0] == 0:
            return
        
        idx = selected_idx[0]
        # 交换位置
        self.selected_components[idx], self.selected_components[idx-1] = self.selected_components[idx-1], self.selected_components[idx]
        # 更新列表
        self.selected_listbox.delete(0, tk.END)
        for comp in self.selected_components:
            self.selected_listbox.insert(tk.END, comp["name"])
        # 重新选中
        self.selected_listbox.selection_set(idx-1)
        self._generate_code()
    
    def _move_down(self):
        """下移组件"""
        selected_idx = self.selected_listbox.curselection()
        if not selected_idx or selected_idx[0] == len(self.selected_components)-1:
            return
        
        idx = selected_idx[0]
        # 交换位置
        self.selected_components[idx], self.selected_components[idx+1] = self.selected_components[idx+1], self.selected_components[idx]
        # 更新列表
        self.selected_listbox.delete(0, tk.END)
        for comp in self.selected_components:
            self.selected_listbox.insert(tk.END, comp["name"])
        # 重新选中
        self.selected_listbox.selection_set(idx+1)
        self._generate_code()
    
    # ========== 代码生成/操作方法 ==========
    def _generate_code(self):
        """生成主程序代码"""
        if not self.selected_components:
            messagebox.showwarning("提示", "请先添加组件！")
            return
        
        # 基础导入代码
        base_imports = """
# 导入项目所有模块（中文文件名）
import 连接数据库
import 设置任务状态并获取任务列表
import 打开谷歌网页
import 获取股票文章页数
import 提取文章列表
import 获取点赞和完整文章发布时间
import 写入股吧帖子数据
import 更新爬取页数

# 导入外部库
import pandas as pd
import datetime
import time

if __name__ == "__main__":
"""
        
        # 拼接组件代码
        component_codes = []
        遍历页码代码 = ""
        遍历文章代码 = ""
        
        # 先收集子组件代码（页码、文章）
        for comp in self.selected_components:
            if comp["name"] == "遍历股票页码":
                # 替换参数
                page_code = comp["code"].replace("{最大爬取页数}", str(self.control_vars["max_pages"].get()))
                遍历页码代码 = page_code
            elif comp["name"] == "遍历页面文章":
                article_code = comp["code"].replace("{最大爬取文章数}", str(self.control_vars["max_articles"].get()))
                遍历文章代码 = article_code
        
        # 拼接主组件代码
        for comp in self.selected_components:
            if comp["name"] == "创建浏览器实例":
                code = comp["code"].replace("{无头模式}", str(self.control_vars["headless"].get()))
            elif comp["name"] == "遍历股票任务":
                code = comp["code"].replace("{最大爬取股票数}", str(self.control_vars["max_stocks"].get()))
                code = code.replace("{遍历页码代码}", 遍历页码代码)
                # 替换文章代码
                code = code.replace("{遍历文章代码}", 遍历文章代码)
            elif comp["name"] not in ["遍历股票页码", "遍历页面文章"]:  # 跳过子组件（已嵌入）
                code = comp["code"]
            else:
                continue  # 子组件不单独输出
            
            # 格式化代码（缩进）
            indented_code = textwrap.indent(code.strip(), "    ")
            component_codes.append(indented_code)
        
        # 合并所有代码
        full_code = base_imports.strip() + "\\n" + "\\n\\n".join(component_codes)
        # 清理多余空行
        full_code = "\n".join([line for line in full_code.split("\n") if line.strip() or line == ""])
        
        # 显示到文本框
        self.code_text.delete(1.0, tk.END)
        self.code_text.insert(1.0, full_code)
    
    def _copy_code(self):
        """复制代码到剪贴板"""
        code = self.code_text.get(1.0, tk.END)
        self.root.clipboard_clear()
        self.root.clipboard_append(code)
        messagebox.showinfo("提示", "代码已复制到剪贴板！")
    
    def _save_code(self):
        """保存代码到文件"""
        code = self.code_text.get(1.0, tk.END)
        try:
            with open("生成的主程序.py", "w", encoding="utf-8") as f:
                f.write(code)
            messagebox.showinfo("提示", "代码已保存到「生成的主程序.py」！")
        except Exception as e:
            messagebox.showerror("错误", f"保存失败：{str(e)}")
    
    # ========== 运行/日志方法 ==========
    def _redirect_stdout(self):
        """重定向stdout到日志文本框"""
        class StdoutRedirector(io.StringIO):
            def __init__(self, text_widget):
                super().__init__()
                self.text_widget = text_widget
            
            def write(self, s):
                super().write(s)
                # 追加到日志，自动滚动
                self.text_widget.insert(tk.END, s)
                self.text_widget.see(tk.END)
                self.text_widget.update_idletasks()
        
        return StdoutRedirector(self.log_text)
    
    def _run_crawler(self):
        """运行生成的爬虫代码"""
        self._clear_log()
        self.log_text.insert(tk.END, f"=== 爬虫开始运行：{datetime.datetime.now()} ===\\n")
        
        # 获取生成的代码
        code = self.code_text.get(1.0, tk.END)
        if not code.strip():
            messagebox.showwarning("提示", "请先生成代码！")
            return
        
        # 重定向输出
        old_stdout = sys.stdout
        old_stderr = sys.stderr
        sys.stdout = self._redirect_stdout()
        sys.stderr = sys.stdout
        
        try:
            # 执行代码
            exec(code, globals())
            self.log_text.insert(tk.END, f"\\n=== 爬虫运行完成：{datetime.datetime.now()} ===\\n")
        except Exception as e:
            error_msg = f"\\n=== 运行出错：{str(e)} ===\\n"
            error_msg += traceback.format_exc()
            self.log_text.insert(tk.END, error_msg)
        finally:
            # 恢复stdout/stderr
            sys.stdout = old_stdout
            sys.stderr = old_stderr
    
    def _clear_log(self):
        """清空日志"""
        self.log_text.delete(1.0, tk.END)

# ========== 程序入口 ==========
if __name__ == "__main__":
    # WSL 下运行 tkinter 需确保 DISPLAY 环境变量正确（连接 Windows 显示）
    if "DISPLAY" not in os.environ:
        print("⚠️  未检测到 DISPLAY 环境变量，尝试自动配置（WSL 默认）")
        os.environ["DISPLAY"] = ":0.0"
    
    # 启动应用
    app = ttkb.Window(themename="flatly")
    editor = CrawlerVisualEditor(app)
    app.mainloop()