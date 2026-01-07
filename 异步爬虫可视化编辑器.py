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
import asyncio

# WSL中文适配
os.environ["LC_ALL"] = "zh_CN.UTF-8"
os.environ["LANG"] = "zh_CN.UTF-8"
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')
if hasattr(sys.stderr, 'reconfigure'):
    sys.stderr.reconfigure(encoding='utf-8')

# 核心组件定义（异步版）
COMPONENTS = [
    {
        "name": "初始化数据库连接",
        "desc": "异步建立数据库连接",
        "params": [],
        "code": """
# 异步初始化数据库连接
数据库连接, 数据库游标 = await 异步数据库连接.建立数据库连接()
if not (数据库连接 and 数据库游标):
    print("❌ 数据库连接失败，程序终止")
    return
"""
    },
    {
        "name": "获取爬取任务列表",
        "desc": "异步读取/补充爬取任务",
        "params": [],
        "code": """
# 异步获取任务列表
任务数据框 = await 异步任务列表管理.初始化并读取爬取任务列表(True)
print(f"\\n📊 任务列表加载完成，共{len(任务数据框)}条任务")
print(任务数据框.head())
"""
    },
    {
        "name": "创建浏览器实例",
        "desc": "异步初始化Playwright浏览器",
        "params": [{"name": "无头模式", "type": "bool", "default": False}],
        "code": """
# 异步创建浏览器实例
p, 浏览器, 上下文 = None, None, None
保持标签页 = None
try:
    p, 浏览器, 上下文 = await 异步浏览器操作.创建浏览器实例(无头模式={无头模式})
    保持标签页 = await 异步浏览器操作.打开新标签页(上下文, "https://guba.eastmoney.com/list,000001_1.html")
"""
    },
    # 其他组件定义（参考同步版，替换为异步函数调用）
    {
        "name": "遍历股票任务",
        "desc": "异步遍历股票代码",
        "params": [{"name": "最大爬取股票数", "type": "int", "default": 2}],
        "code": """
# 异步遍历股票代码
控制变量1 = 0
目标列 = "stock_code"
for 股票代码 in 任务数据框[目标列]:
    控制变量1 += 1
    第一层网页链接 = f"https://guba.eastmoney.com/list,{股票代码}_1.html"
    
    try:
        主标签页 = await 异步浏览器操作.打开新标签页(上下文, 第一层网页链接)
        总页数 = await 异步获取股吧总页数.获取股吧总页数(主标签页)
        if not 总页数:
            print(f"⚠️  无法获取{股票代码}总页数，跳过")
            await 异步浏览器操作.关闭单个标签页(主标签页)
            continue
        print(f"📌 股票{股票代码}总页数：{总页数}")
        
        # 遍历页码
        控制变量2 = 0
        {遍历页码代码}
        
        await 异步浏览器操作.关闭单个标签页(主标签页)
        
    except Exception as e:
        print(f"\n❌ 股票首页失败：{str(e)}")
    
    if 控制变量1 == {最大爬取股票数}:
        break
"""
    },
    # 剩余组件（遍历页码、文章、资源释放）参考同步版改造为异步
    {
        "name": "资源释放",
        "desc": "异步关闭浏览器/数据库连接",
        "params": [],
        "code": """
# 异步释放资源
await asyncio.sleep(1)

except Exception as e:
    print(f"\n❌ 浏览器操作失败：{str(e)}")
finally:
    if 保持标签页:
        await 异步浏览器操作.关闭单个标签页(保持标签页)
    if all([p, 浏览器, 上下文]):
        await 异步浏览器操作.关闭浏览器实例(p, 浏览器, 上下文)

except Exception as e:
    print(f"\n❌ 主程序失败：{str(e)}")
finally:
    await 异步数据库连接.关闭数据库连接(数据库连接, 数据库游标)
    print("\\n🎉 异步爬虫运行结束")
"""
    }
]

class 异步爬虫可视化编辑器:
    def __init__(self, root):
        self.root = root
        self.root.title("东方财富股吧异步爬虫可视化编辑器")
        self.root.geometry("1200x800")
        
        # 样式配置
        style = ttkb.Style()
        style.configure(".", font=("Noto Sans CJK SC", 10), foreground="#333", background="#f8f9fa")
        style.configure("Bold.TLabel", font=("Noto Sans CJK SC", 12, "bold"))
        
        # 初始化变量
        self.selected_components = []
        self.component_params = {}
        self.control_vars = {
            "max_stocks": tk.IntVar(value=2),
            "max_pages": tk.IntVar(value=2),
            "max_articles": tk.IntVar(value=90),
            "headless": tk.BooleanVar(value=False)
        }
        
        # 创建界面（同同步版，仅修改运行逻辑）
        self._create_widgets()
    
    def _create_widgets(self):
        # 主容器
        main_paned = ttkb.PanedWindow(self.root, orient=tk.HORIZONTAL)
        main_paned.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # 左侧面板（配置+组件选择）
        left_frame = ttkb.Frame(main_paned, width=300)
        main_paned.add(left_frame, weight=1)
        
        # 配置面板
        config_label = ttkb.Label(left_frame, text="⚙️ 基础配置", style="Bold.TLabel")
        config_label.pack(anchor=tk.W, padx=5, pady=5)
        
        control_frame = ttkb.LabelFrame(left_frame, text="爬取控制", padding=5)
        control_frame.pack(fill=tk.X, padx=5, pady=3)
        
        ttkb.Label(control_frame, text="最大爬取股票数：").grid(row=0, column=0, sticky=tk.W, padx=2, pady=2)
        ttkb.Entry(control_frame, textvariable=self.control_vars["max_stocks"], width=10).grid(row=0, column=1, padx=2, pady=2)
        
        ttkb.Label(control_frame, text="单股票最大页数：").grid(row=1, column=0, sticky=tk.W, padx=2, pady=2)
        ttkb.Entry(control_frame, textvariable=self.control_vars["max_pages"], width=10).grid(row=1, column=1, padx=2, pady=2)
        
        ttkb.Label(control_frame, text="单页最大文章数：").grid(row=2, column=0, sticky=tk.W, padx=2, pady=2)
        ttkb.Entry(control_frame, textvariable=self.control_vars["max_articles"], width=10).grid(row=2, column=1, padx=2, pady=2)
        
        ttkb.Checkbutton(control_frame, text="浏览器无头模式", variable=self.control_vars["headless"]).grid(row=3, column=0, columnspan=2, sticky=tk.W, padx=2, pady=2)
        
        # 组件选择
        component_label = ttkb.Label(left_frame, text="🧩 功能组件", style="Bold.TLabel")
        component_label.pack(anchor=tk.W, padx=5, pady=5)
        
        self.component_listbox = tk.Listbox(left_frame, height=10, font=("Noto Sans CJK SC", 10))
        self.component_listbox.pack(fill=tk.BOTH, expand=True, padx=5, pady=3)
        for comp in COMPONENTS:
            self.component_listbox.insert(tk.END, comp["name"])
        
        # 组件操作按钮
        btn_frame = ttkb.Frame(left_frame)
        btn_frame.pack(fill=tk.X, padx=5, pady=3)
        
        ttkb.Button(btn_frame, text="添加组件", command=self._add_component, bootstyle="success").pack(side=tk.LEFT, padx=2)
        ttkb.Button(btn_frame, text="移除组件", command=self._remove_component, bootstyle="danger").pack(side=tk.LEFT, padx=2)
        ttkb.Button(btn_frame, text="上移", command=self._move_up).pack(side=tk.LEFT, padx=2)
        ttkb.Button(btn_frame, text="下移", command=self._move_down).pack(side=tk.LEFT, padx=2)
        
        # 已选组件
        selected_label = ttkb.Label(left_frame, text="📋 已选组件（执行顺序）", style="Bold.TLabel")
        selected_label.pack(anchor=tk.W, padx=5, pady=5)
        
        self.selected_listbox = tk.Listbox(left_frame, height=8, font=("Noto Sans CJK SC", 10))
        self.selected_listbox.pack(fill=tk.BOTH, expand=True, padx=5, pady=3)
        
        # 中间面板（代码预览）
        middle_frame = ttkb.Frame(main_paned, width=400)
        main_paned.add(middle_frame, weight=2)
        
        code_label = ttkb.Label(middle_frame, text="📝 生成的异步代码", style="Bold.TLabel")
        code_label.pack(anchor=tk.W, padx=5, pady=5)
        
        self.code_text = ScrolledText(middle_frame, font=("DejaVu Sans Mono, Noto Sans CJK SC", 10), wrap=tk.NONE)
        self.code_text.pack(fill=tk.BOTH, expand=True, padx=5, pady=3)
        
        # 代码操作按钮
        code_btn_frame = ttkb.Frame(middle_frame)
        code_btn_frame.pack(fill=tk.X, padx=5, pady=3)
        
        ttkb.Button(code_btn_frame, text="生成代码", command=self._generate_code, bootstyle="primary").pack(side=tk.LEFT, padx=2)
        ttkb.Button(code_btn_frame, text="复制代码", command=self._copy_code, bootstyle="info").pack(side=tk.LEFT, padx=2)
        ttkb.Button(code_btn_frame, text="保存代码", command=self._save_code, bootstyle="warning").pack(side=tk.LEFT, padx=2)
        
        # 右侧面板（运行日志）
        right_frame = ttkb.Frame(main_paned, width=300)
        main_paned.add(right_frame, weight=1)
        
        log_label = ttkb.Label(right_frame, text="📜 运行日志", style="Bold.TLabel")
        log_label.pack(anchor=tk.W, padx=5, pady=5)
        
        self.log_text = ScrolledText(right_frame, font=("DejaVu Sans Mono, Noto Sans CJK SC", 10), wrap=tk.WORD)
        self.log_text.pack(fill=tk.BOTH, expand=True, padx=5, pady=3)
        
        # 运行按钮
        run_btn_frame = ttkb.Frame(right_frame)
        run_btn_frame.pack(fill=tk.X, padx=5, pady=3)
        
        ttkb.Button(run_btn_frame, text="运行异步爬虫", command=self._run_crawler, bootstyle="success outline").pack(side=tk.LEFT, padx=2)
        ttkb.Button(run_btn_frame, text="清空日志", command=self._clear_log, bootstyle="danger outline").pack(side=tk.LEFT, padx=2)
    
    # 组件操作方法（同同步版）
    def _add_component(self):
        selected_idx = self.component_listbox.curselection()
        if not selected_idx:
            messagebox.showwarning("提示", "请选择组件！")
            return
        comp_idx = selected_idx[0]
        comp = COMPONENTS[comp_idx]
        self.selected_components.append(comp)
        self.selected_listbox.insert(tk.END, comp["name"])
        self._generate_code()
    
    def _remove_component(self):
        selected_idx = self.selected_listbox.curselection()
        if not selected_idx:
            messagebox.showwarning("提示", "请选择要移除的组件！")
            return
        idx = selected_idx[0]
        del self.selected_components[idx]
        self.selected_listbox.delete(idx)
        self._generate_code()
    
    def _move_up(self):
        selected_idx = self.selected_listbox.curselection()
        if not selected_idx or selected_idx[0] == 0:
            return
        idx = selected_idx[0]
        self.selected_components[idx], self.selected_components[idx-1] = self.selected_components[idx-1], self.selected_components[idx]
        self.selected_listbox.delete(0, tk.END)
        for comp in self.selected_components:
            self.selected_listbox.insert(tk.END, comp["name"])
        self.selected_listbox.selection_set(idx-1)
        self._generate_code()
    
    def _move_down(self):
        selected_idx = self.selected_listbox.curselection()
        if not selected_idx or selected_idx[0] == len(self.selected_components)-1:
            return
        idx = selected_idx[0]
        self.selected_components[idx], self.selected_components[idx+1] = self.selected_components[idx+1], self.selected_components[idx]
        self.selected_listbox.delete(0, tk.END)
        for comp in self.selected_components:
            self.selected_listbox.insert(tk.END, comp["name"])
        self.selected_listbox.selection_set(idx+1)
        self._generate_code()
    
    # 代码生成（异步版）
    def _generate_code(self):
        if not self.selected_components:
            messagebox.showwarning("提示", "请添加组件！")
            return
        
        # 异步基础导入
        base_imports = """
# 异步爬虫基础导入
import asyncio
import pandas as pd
import datetime
import 异步数据库连接
import 异步任务列表管理
import 异步浏览器操作
import 异步获取股吧总页数
import 异步提取文章列表
import 异步提取文章详情
import 异步写入帖子数据
import 异步更新爬取进度

async def 异步爬取主函数():
"""
        
        # 拼接组件代码
        component_codes = []
        遍历页码代码 = ""
        遍历文章代码 = ""
        
        # 收集子组件代码
        for comp in self.selected_components:
            if comp["name"] == "遍历股票页码":
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
                code = code.replace("{遍历文章代码}", 遍历文章代码)
            elif comp["name"] not in ["遍历股票页码", "遍历页面文章"]:
                code = comp["code"]
            else:
                continue
            
            # 缩进代码
            indented_code = textwrap.indent(code.strip(), "    ")
            component_codes.append(indented_code)
        
        # 合并代码
        full_code = base_imports.strip() + "\\n" + "\\n\\n".join(component_codes)
        full_code += "\\n\\nif __name__ == '__main__':\\n    asyncio.run(异步爬取主函数())"
        full_code = "\n".join([line for line in full_code.split("\n") if line.strip() or line == ""])
        
        # 显示代码
        self.code_text.delete(1.0, tk.END)
        self.code_text.insert(1.0, full_code)
    
    def _copy_code(self):
        code = self.code_text.get(1.0, tk.END)
        self.root.clipboard_clear()
        self.root.clipboard_append(code)
        messagebox.showinfo("提示", "代码已复制！")
    
    def _save_code(self):
        code = self.code_text.get(1.0, tk.END)
        try:
            with open("生成的异步主程序.py", "w", encoding="utf-8") as f:
                f.write(code)
            messagebox.showinfo("提示", "代码已保存到「生成的异步主程序.py」！")
        except Exception as e:
            messagebox.showerror("错误", f"保存失败：{str(e)}")
    
    # 异步运行爬虫（核心改造）
    def _redirect_stdout(self):
        class StdoutRedirector(io.StringIO):
            def __init__(self, text_widget):
                super().__init__()
                self.text_widget = text_widget
            
            def write(self, s):
                super().write(s)
                self.text_widget.insert(tk.END, s)
                self.text_widget.see(tk.END)
                self.text_widget.update_idletasks()
        
        return StdoutRedirector(self.log_text)
    
    def _run_crawler(self):
        """异步运行爬虫（在新线程中执行，避免UI阻塞）"""
        self._clear_log()
        self.log_text.insert(tk.END, f"=== 异步爬虫开始运行：{datetime.datetime.now()} ===\\n")
        
        code = self.code_text.get(1.0, tk.END)
        if not code.strip():
            messagebox.showwarning("提示", "请先生成代码！")
            return
        
        # 重定向输出
        old_stdout = sys.stdout
        old_stderr = sys.stderr
        sys.stdout = self._redirect_stdout()
        sys.stderr = sys.stdout
        
        # 异步运行函数
        def run_async_code():
            try:
                # 执行异步代码
                exec(code, globals())
                self.log_text.insert(tk.END, f"\\n=== 异步爬虫运行完成：{datetime.datetime.now()} ===\\n")
            except Exception as e:
                error_msg = f"\\n=== 运行出错：{str(e)} ===\\n"
                error_msg += traceback.format_exc()
                self.log_text.insert(tk.END, error_msg)
            finally:
                sys.stdout = old_stdout
                sys.stderr = old_stderr
        
        # 在新线程中运行，避免UI卡死
        import threading
        threading.Thread(target=run_async_code, daemon=True).start()
    
    def _clear_log(self):
        self.log_text.delete(1.0, tk.END)

if __name__ == "__main__":
    if "DISPLAY" not in os.environ:
        os.environ["DISPLAY"] = ":0.0"
    
    app = ttkb.Window(themename="flatly")
    editor = 异步爬虫可视化编辑器(app)
    app.mainloop()