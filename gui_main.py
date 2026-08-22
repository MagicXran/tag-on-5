"""
RMS标签导入处理系统 - GUI界面
提供用户友好的界面让用户选择目录并执行数据处理
"""

import tkinter as tk
from tkinter import ttk, filedialog, messagebox, scrolledtext
from pathlib import Path
import threading
import sys
import os
import uuid
from datetime import datetime

# 导入现有的处理器
from main_processor import RMSTagOnProcessor
from transactions_processor import TransactionsProcessor
from logger_utils import LoggerManager


class RMSProcessorGUI:
    """RMS处理器GUI界面"""
    
    def __init__(self, root):
        self.root = root
        self.root.title("RMS标签导入处理系统")
        self.root.geometry("800x700")
        
        # 初始化处理器
        self.main_processor = RMSTagOnProcessor()
        self.transactions_processor = TransactionsProcessor(
            db_manager=self.main_processor.database_manager,
            third_party_client=self.main_processor.third_party_client,
        )
        
        # 用于控制处理线程
        self.processing_thread = None
        self.is_processing = False
        
        # 设置界面
        self.setup_ui()
        
    def setup_ui(self):
        """设置用户界面"""
        # 主标题
        title_label = tk.Label(
            self.root, 
            text="RMS标签导入处理系统", 
            font=("Arial", 16, "bold"),
            fg="blue"
        )
        title_label.pack(pady=10)
        
        # 创建主框架
        main_frame = ttk.Frame(self.root)
        main_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)
        
        # 目录选择区域
        self.setup_directory_section(main_frame)
        
        # 处理选项区域
        self.setup_options_section(main_frame)
        
        # 按钮区域
        self.setup_buttons_section(main_frame)
        
        # 日志输出区域
        self.setup_log_section(main_frame)
        
        # 状态栏
        self.setup_status_bar()
        
    def setup_directory_section(self, parent):
        """设置目录选择区域"""
        dir_frame = ttk.LabelFrame(parent, text="目录设置", padding=10)
        dir_frame.pack(fill=tk.X, pady=5)
        
        # 合同目录
        ttk.Label(dir_frame, text="合同目录:").grid(row=0, column=0, sticky=tk.W, pady=5)
        self.contracts_path = tk.StringVar()
        contracts_entry = ttk.Entry(dir_frame, textvariable=self.contracts_path, width=60)
        contracts_entry.grid(row=0, column=1, padx=5, pady=5, sticky=tk.EW)
        ttk.Button(
            dir_frame, 
            text="浏览", 
            command=lambda: self.browse_directory(self.contracts_path)
        ).grid(row=0, column=2, padx=5, pady=5)
        
        # 经费目录
        ttk.Label(dir_frame, text="经费目录:").grid(row=1, column=0, sticky=tk.W, pady=5)
        self.funds_path = tk.StringVar()
        funds_entry = ttk.Entry(dir_frame, textvariable=self.funds_path, width=60)
        funds_entry.grid(row=1, column=1, padx=5, pady=5, sticky=tk.EW)
        ttk.Button(
            dir_frame, 
            text="浏览", 
            command=lambda: self.browse_directory(self.funds_path)
        ).grid(row=1, column=2, padx=5, pady=5)
        
        # 收支明细目录
        ttk.Label(dir_frame, text="收支明细目录:").grid(row=2, column=0, sticky=tk.W, pady=5)
        self.transactions_path = tk.StringVar()
        transactions_entry = ttk.Entry(dir_frame, textvariable=self.transactions_path, width=60)
        transactions_entry.grid(row=2, column=1, padx=5, pady=5, sticky=tk.EW)
        ttk.Button(
            dir_frame, 
            text="浏览", 
            command=lambda: self.browse_directory(self.transactions_path)
        ).grid(row=2, column=2, padx=5, pady=5)
        
        # 配置列权重
        dir_frame.columnconfigure(1, weight=1)
        
    def setup_options_section(self, parent):
        """设置处理选项区域"""
        options_frame = ttk.LabelFrame(parent, text="处理选项", padding=10)
        options_frame.pack(fill=tk.X, pady=5)
        
        # 处理类型选择
        self.process_contracts_data = tk.BooleanVar(value=True)
        self.process_funds_data = tk.BooleanVar(value=True)
        self.process_transactions_data = tk.BooleanVar(value=True)
        self.enable_third_party_push = tk.BooleanVar(value=True)
        
        ttk.Checkbutton(
            options_frame, 
            text="处理合同数据", 
            variable=self.process_contracts_data
        ).grid(row=0, column=0, sticky=tk.W, pady=2, padx=10)
        
        ttk.Checkbutton(
            options_frame, 
            text="处理经费数据", 
            variable=self.process_funds_data
        ).grid(row=0, column=1, sticky=tk.W, pady=2, padx=10)
        
        ttk.Checkbutton(
            options_frame, 
            text="处理收支明细数据", 
            variable=self.process_transactions_data
        ).grid(row=1, column=0, sticky=tk.W, pady=2, padx=10)
        
        ttk.Checkbutton(
            options_frame, 
            text="保存后推送第三方系统",
            variable=self.enable_third_party_push
        ).grid(row=1, column=1, sticky=tk.W, pady=2, padx=10)
        
    def setup_buttons_section(self, parent):
        """设置按钮区域"""
        button_frame = ttk.Frame(parent)
        button_frame.pack(fill=tk.X, pady=10)
        
        # 开始处理按钮
        self.start_button = ttk.Button(
            button_frame, 
            text="开始处理", 
            command=self.start_processing,
            style="Start.TButton"
        )
        self.start_button.pack(side=tk.LEFT, padx=5)
        
        # 停止处理按钮
        self.stop_button = ttk.Button(
            button_frame, 
            text="停止处理", 
            command=self.stop_processing,
            state=tk.DISABLED
        )
        self.stop_button.pack(side=tk.LEFT, padx=5)
        
        # 清空日志按钮
        ttk.Button(
            button_frame, 
            text="清空日志", 
            command=self.clear_log
        ).pack(side=tk.LEFT, padx=5)
        
        # 环境测试按钮
        ttk.Button(
            button_frame, 
            text="环境测试", 
            command=self.test_environment
        ).pack(side=tk.RIGHT, padx=5)
        
        # 详细诊断按钮
        ttk.Button(
            button_frame, 
            text="详细诊断", 
            command=self.detailed_diagnosis
        ).pack(side=tk.RIGHT, padx=5)

        # 配置管理按钮
        ttk.Button(
            button_frame, 
            text="查看配置", 
            command=self.view_config
        ).pack(side=tk.RIGHT, padx=5)
        
        # 刷新连接按钮
        ttk.Button(
            button_frame, 
            text="刷新连接", 
            command=self.refresh_connection
        ).pack(side=tk.RIGHT, padx=5)
        
        # 数据库管理按钮
        ttk.Button(
            button_frame, 
            text="数据库管理", 
            command=self.open_database_management
        ).pack(side=tk.RIGHT, padx=5)
        
    def open_database_management(self):
        """打开数据库管理界面"""
        try:
            # 导入数据库管理界面
            from database_management_gui import show_database_management
            show_database_management(self.root)
        except ImportError:
            print('数据库管理界面模块未找到')
            messagebox.showerror("错误", "数据库管理界面模块未找到")
        except Exception as e:
            print('打开数据库管理界面失败')
            messagebox.showerror("错误", f"打开数据库管理界面失败: {str(e)}")
            
    def view_config(self):
        """查看当前配置"""
        self.log_message("=== 当前配置信息 ===")
        
        # 显示数据库配置
        self.log_message("数据库配置:")
        db_config = self.main_processor.database_manager.config
        for key, value in db_config.items():
            if key == 'password':
                self.log_message(f"  {key}: {'*' * len(str(value))}")  # 隐藏密码
            else:
                self.log_message(f"  {key}: {value}")
        
        # 显示第三方推送配置
        self.log_message("第三方推送配置:")
        try:
            push_config = self.main_processor.third_party_client.config
            for key, value in push_config.items():
                self.log_message(f"  {key}: {value}")
        except Exception as e:
            self.log_message(f"获取第三方配置失败: {str(e)}", "ERROR")
        
        self.log_message("=== 配置信息显示完成 ===")

    def refresh_connection(self):
        """刷新数据库连接"""
        self.log_message("刷新数据库连接...")
        
        def run_refresh():
            try:
                self.main_processor.database_manager.refresh_connection()
                self.log_message("数据库连接刷新成功", "SUCCESS")
                
                # 重新测试连接
                connection_success, connection_message = self.main_processor.database_manager.test_connection()
                if connection_success:
                    self.log_message(f"连接测试成功: {connection_message}", "SUCCESS")
                else:
                    self.log_message(f"连接测试失败: {connection_message}", "ERROR")
                    
            except Exception as e:
                self.log_message(f"刷新连接失败: {str(e)}", "ERROR")
                
        # 在后台线程中运行刷新
        threading.Thread(target=run_refresh, daemon=True).start()
        
    def setup_log_section(self, parent):
        """设置日志输出区域"""
        log_frame = ttk.LabelFrame(parent, text="处理日志", padding=5)
        log_frame.pack(fill=tk.BOTH, expand=True, pady=5)
        
        # 创建日志文本框
        self.log_text = scrolledtext.ScrolledText(
            log_frame, 
            height=15, 
            font=("Consolas", 9),
            wrap=tk.WORD
        )
        self.log_text.pack(fill=tk.BOTH, expand=True)
        
    def setup_status_bar(self):
        """设置状态栏"""
        self.status_var = tk.StringVar()
        self.status_var.set("就绪")
        
        status_bar = ttk.Label(
            self.root, 
            textvariable=self.status_var, 
            relief=tk.SUNKEN, 
            anchor=tk.W
        )
        status_bar.pack(side=tk.BOTTOM, fill=tk.X)
        
    def browse_directory(self, path_var):
        """浏览目录对话框"""
        directory = filedialog.askdirectory()
        if directory:
            path_var.set(directory)
            
    def log_message(self, message, level="INFO"):
        """添加日志消息"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        formatted_message = f"[{timestamp}] {level}: {message}\n"
        
        # 在GUI线程中更新UI
        self.root.after(0, self._append_log, formatted_message, level)
        
    def _append_log(self, message, level):
        """在GUI线程中添加日志"""
        self.log_text.insert(tk.END, message)
        
        # 根据日志级别设置颜色
        if level == "ERROR":
            # 找到刚插入的文本并设置颜色
            start_index = self.log_text.index(f"end-{len(message)}c")
            self.log_text.tag_add("error", start_index, "end-1c")
            self.log_text.tag_config("error", foreground="red")
        elif level == "WARNING":
            start_index = self.log_text.index(f"end-{len(message)}c")
            self.log_text.tag_add("warning", start_index, "end-1c")
            self.log_text.tag_config("warning", foreground="orange")
        elif level == "SUCCESS":
            start_index = self.log_text.index(f"end-{len(message)}c")
            self.log_text.tag_add("success", start_index, "end-1c")
            self.log_text.tag_config("success", foreground="green")
            
        # 自动滚动到底部
        self.log_text.see(tk.END)
        
    def clear_log(self):
        """清空日志"""
        self.log_text.delete(1.0, tk.END)
        
    def test_environment(self):
        """测试环境配置"""
        self.log_message("开始环境测试...")
        
        def run_test():
            try:
                # 首先验证数据库配置
                self.log_message("验证数据库配置...")
                config_valid, config_message = self.main_processor.database_manager.validate_config()
                
                if not config_valid:
                    self.log_message(f"数据库配置无效: {config_message}", "ERROR")
                    return
                
                self.log_message(f"数据库配置验证: {config_message}", "SUCCESS")
                
                # 测试数据库连接
                self.log_message("测试数据库连接...")
                self.log_message("数据库配置信息:")
                self.log_message(
                    f"  SQLite文件: {self.main_processor.database_manager.config['database']}"
                )
                
                # 调用改进的test_connection方法
                connection_success, connection_message = self.main_processor.database_manager.test_connection()
                
                if connection_success:
                    self.log_message(f"数据库连接成功: {connection_message}", "SUCCESS")
                else:
                    self.log_message(f"数据库连接失败: {connection_message}", "ERROR")
                    
                if self.enable_third_party_push.get():
                    self.log_message(f"第三方接口地址: {self.main_processor.third_party_client.url}")
                    self.log_message("第三方接口将在正式推送时验证", "INFO")
                    
                self.log_message("环境测试完成", "SUCCESS")
                
            except Exception as e:
                self.log_message(f"环境测试失败: {str(e)}", "ERROR")
                import traceback
                error_details = traceback.format_exc()
                self.log_message(f"详细错误信息:\n{error_details}", "ERROR")
                
        # 在后台线程中运行测试
        threading.Thread(target=run_test, daemon=True).start()
        
    def validate_inputs(self):
        """验证用户输入"""
        # 检查是否至少选择了一种数据类型
        if not (self.process_contracts_data.get() or self.process_funds_data.get() or self.process_transactions_data.get()):
            messagebox.showerror("错误", "请至少选择一种数据类型进行处理")
            return False
        
        # 检查合同数据处理
        if self.process_contracts_data.get():
            if not self.contracts_path.get():
                messagebox.showerror("错误", "已选择处理合同数据，请选择合同目录")
                return False
            if not os.path.exists(self.contracts_path.get()):
                messagebox.showerror("错误", "合同目录不存在")
                return False
        
        # 检查经费数据处理
        if self.process_funds_data.get():
            if not self.funds_path.get():
                messagebox.showerror("错误", "已选择处理经费数据，请选择经费目录")
                return False
            if not os.path.exists(self.funds_path.get()):
                messagebox.showerror("错误", "经费目录不存在")
                return False
        
        # 检查收支明细数据处理
        if self.process_transactions_data.get():
            if not self.transactions_path.get():
                messagebox.showerror("错误", "已选择处理收支明细数据，请选择收支明细目录")
                return False
            if not os.path.exists(self.transactions_path.get()):
                messagebox.showerror("错误", "收支明细目录不存在")
                return False
        
        return True
        
    def start_processing(self):
        """开始处理数据"""
        if not self.validate_inputs():
            return
            
        if self.processing_thread and self.processing_thread.is_alive():
            messagebox.showwarning("警告", "处理正在进行中，请稍候...")
            return
            
        # 更新UI状态
        self.is_processing = True
        self.start_button.config(state=tk.DISABLED)
        self.stop_button.config(state=tk.NORMAL)
        self.status_var.set("处理中...")
        
        # 清空日志
        self.clear_log()
        
        # 启动处理线程
        self.processing_thread = threading.Thread(target=self.run_processing, daemon=True)
        self.processing_thread.start()
        
    def stop_processing(self):
        """停止处理"""
        self.is_processing = False
        self.log_message("正在停止处理...", "WARNING")
        
    def run_processing(self):
        """运行数据处理（在后台线程中）"""
        try:
            run_id = str(uuid.uuid4())
            self.log_message(f"开始数据处理，run_id={run_id}")
            overall_success = True
            
            # 处理合同数据
            if self.process_contracts_data.get() and self.is_processing:
                self.log_message("开始处理合同数据...")
                overall_success = self.process_contracts_data_only() and overall_success
            
            # 处理经费数据
            if self.process_funds_data.get() and self.is_processing:
                self.log_message("开始处理经费数据...")
                overall_success = self.process_funds_data_only() and overall_success
            
            # 处理收支明细数据
            if self.process_transactions_data.get() and self.is_processing:
                self.log_message("开始处理收支明细数据...")
                overall_success = self.process_transactions_data_only() and overall_success
            
            if self.is_processing:
                if overall_success:
                    self.log_message("所有数据处理完成！", "SUCCESS")
                    self.status_var.set("处理完成")
                else:
                    self.log_message("数据已保存，但存在文件或第三方推送失败", "WARNING")
                    self.status_var.set("处理完成（有错误）")
            else:
                self.log_message("处理已被用户停止", "WARNING")
                self.status_var.set("处理已停止")
                
        except Exception as e:
            self.log_message(f"处理过程中发生错误: {str(e)}", "ERROR")
            self.status_var.set("处理出错")
            
        finally:
            # 恢复UI状态
            self.root.after(0, self.reset_ui_state)

    def detailed_diagnosis(self):
        """详细诊断系统状态"""
        self.log_message("开始详细诊断...")
        
        def run_diagnosis():
            try:
                # 系统信息
                self.log_message("=== 系统信息 ===")
                self.log_message(f"Python版本: {sys.version}")
                self.log_message(f"操作系统: {os.name}")
                
                # 模块版本信息
                self.log_message("=== 模块版本信息 ===")
                import sqlite3
                self.log_message(f"SQLite版本: {sqlite3.sqlite_version}")
                
                try:
                    import pandas as pd
                    self.log_message(f"Pandas版本: {pd.__version__}")
                except ImportError as e:
                    self.log_message(f"Pandas导入失败: {str(e)}", "ERROR")
                
                # 数据库诊断
                self.log_message("=== 数据库诊断 ===")
                diagnostic_info = self.main_processor.database_manager.get_connection_diagnostic_info()
                
                self.log_message(f"配置验证: {'通过' if diagnostic_info['config_valid'] else '失败'}")
                self.log_message(f"配置信息: {diagnostic_info['config_message']}")
                self.log_message(f"连接测试: {'成功' if diagnostic_info['connection_test'] else '失败'}")
                self.log_message(f"连接信息: {diagnostic_info['connection_message']}")
                
                if diagnostic_info['detailed_error']:
                    self.log_message(f"详细错误: {diagnostic_info['detailed_error']}", "ERROR")
                
                # 文件路径检查
                self.log_message("=== 文件路径检查 ===")
                
                # 检查合同目录
                if self.contracts_path.get():
                    path_exists = os.path.exists(self.contracts_path.get())
                    self.log_message(f"合同目录: {self.contracts_path.get()}")
                    self.log_message(f"目录存在: {'是' if path_exists else '否'}")
                    
                    if path_exists:
                        excel_files = self.get_excel_files(self.contracts_path.get())
                        self.log_message(f"Excel文件数量: {len(excel_files)}")
                        for i, file in enumerate(excel_files[:5]):  # 只显示前5个
                            self.log_message(f"  文件{i+1}: {os.path.basename(file)}")
                        if len(excel_files) > 5:
                            self.log_message(f"  ...还有{len(excel_files)-5}个文件")
                
                # 检查经费目录
                if self.funds_path.get():
                    path_exists = os.path.exists(self.funds_path.get())
                    self.log_message(f"经费目录: {self.funds_path.get()}")
                    self.log_message(f"目录存在: {'是' if path_exists else '否'}")
                    
                    if path_exists:
                        excel_files = self.get_excel_files(self.funds_path.get())
                        self.log_message(f"Excel文件数量: {len(excel_files)}")
                        for i, file in enumerate(excel_files[:5]):  # 只显示前5个
                            self.log_message(f"  文件{i+1}: {os.path.basename(file)}")
                        if len(excel_files) > 5:
                            self.log_message(f"  ...还有{len(excel_files)-5}个文件")
                
                if self.transactions_path.get():
                    path_exists = os.path.exists(self.transactions_path.get())
                    self.log_message(f"收支明细目录: {self.transactions_path.get()}")
                    self.log_message(f"目录存在: {'是' if path_exists else '否'}")
                    
                    if path_exists:
                        excel_files = self.get_excel_files(self.transactions_path.get())
                        self.log_message(f"Excel文件数量: {len(excel_files)}")
                        for i, file in enumerate(excel_files[:5]):  # 只显示前5个
                            self.log_message(f"  文件{i+1}: {os.path.basename(file)}")
                        if len(excel_files) > 5:
                            self.log_message(f"  ...还有{len(excel_files)-5}个文件")
                
                self.log_message("=== 诊断完成 ===", "SUCCESS")
                
            except Exception as e:
                self.log_message(f"诊断过程中发生错误: {str(e)}", "ERROR")
                import traceback
                error_details = traceback.format_exc()
                self.log_message(f"详细错误信息:\n{error_details}", "ERROR")
                
        # 在后台线程中运行诊断
        threading.Thread(target=run_diagnosis, daemon=True).start()
            
    def reset_ui_state(self):
        """重置UI状态"""
        self.is_processing = False
        self.start_button.config(state=tk.NORMAL)
        self.stop_button.config(state=tk.DISABLED)
        if self.status_var.get() not in ["处理完成", "处理完成（有错误）", "处理出错", "处理已停止"]:
            self.status_var.set("就绪")
            
    def get_excel_files(self, folder_path):
        """获取文件夹中的所有Excel文件"""
        excel_extensions = ['.xlsx', '.xls']
        excel_files = []
        
        for root, dirs, files in os.walk(folder_path):
            for file in files:
                if any(file.lower().endswith(ext) for ext in excel_extensions):
                    excel_files.append(os.path.join(root, file))
                    
        return excel_files

    def process_contracts_data_only(self):
        """只处理合同数据"""
        contracts_folder = self.contracts_path.get()
        try:
            results = self.main_processor.process_contracts_folder(
                contracts_folder, push=self.enable_third_party_push.get()
            )
            level = "SUCCESS" if results.get("success") else "WARNING"
            self.log_message(
                f"合同数据处理完成: 处理 {results['processed_files']} 个文件，"
                f"插入 {results['total_inserted']} 条，更新 {results['total_updated']} 条",
                level,
            )
            self._log_push_result("合同", results.get("push_result"))
            for error in results.get('errors', []):
                self.log_message(f"合同处理错误: {error}", "ERROR")
            return bool(results.get("success"))
        except Exception as e:
            self.log_message(f"合同数据处理失败: {str(e)}", "ERROR")
            return False
    
    def process_funds_data_only(self):
        """只处理经费数据"""
        funds_folder = self.funds_path.get()
        try:
            results = self.main_processor.process_project_funds_folder(
                funds_folder, push=self.enable_third_party_push.get()
            )
            level = "SUCCESS" if results.get("success") else "WARNING"
            self.log_message(
                f"经费数据处理完成: 处理 {results['processed_files']} 个文件，"
                f"插入 {results['total_inserted']} 条，更新 {results['total_updated']} 条",
                level,
            )
            self._log_push_result("经费到账", results.get("push_result"))
            for error in results.get('errors', []):
                self.log_message(f"经费处理错误: {error}", "ERROR")
            return bool(results.get("success"))
        except Exception as e:
            self.log_message(f"经费数据处理失败: {str(e)}", "ERROR")
            return False
    
    def process_transactions_data_only(self):
        """只处理收支明细数据"""
        transactions_folder = self.transactions_path.get()
        try:
            results = self.transactions_processor.process_transactions_folder(
                transactions_folder, push=self.enable_third_party_push.get()
            )
            failed_files = results.get('failed_files', 0)
            has_errors = bool(results.get('errors'))
            level = "WARNING" if has_errors else "SUCCESS"
            total_files = results['processed_files'] + failed_files
            self.log_message(
                f"收支明细处理完成: 共{total_files}个文件，"
                f"成功{results['processed_files']}个({results['successful_records']}条记录)，"
                f"失败{failed_files}个",
                level
            )
            self._log_push_result("经费收支", results.get("push_result"))
            if has_errors:
                for error in results['errors']:
                    self.log_message(f"  错误: {error}", "ERROR")
            return bool(results.get("success"))
        except Exception as e:
            self.log_message(f"收支明细处理失败: {str(e)}", "ERROR")
            return False

    def _log_push_result(self, label, push_result):
        if not self.enable_third_party_push.get():
            self.log_message(f"{label}第三方推送已禁用", "INFO")
        elif not push_result:
            self.log_message(f"{label}无数据需要推送", "INFO")
        elif push_result.get("success"):
            self.log_message(
                f"{label}第三方推送成功，批次{len(push_result.get('batches', []))}个，"
                f"请求尝试{push_result.get('attempts', 0)}次",
                "SUCCESS",
            )
        else:
            self.log_message(
                f"{label} SQLite保存成功、第三方推送失败，"
                f"失败批次{push_result.get('failed_batches', 0)}个",
                "ERROR",
            )
def main():
    """主函数"""
    root = tk.Tk()
    
    # 设置主题样式
    style = ttk.Style()
    style.theme_use('clam')  # 使用现代主题
    
    # 创建GUI应用
    app = RMSProcessorGUI(root)
    
    # 启动GUI事件循环
    root.mainloop()


if __name__ == "__main__":
    main()
