"""
数据库管理GUI模块 - 完全类型安全版本
提供数据库表的查看、清除等管理功能
"""

import tkinter as tk
from tkinter import ttk, messagebox, filedialog
import pandas as pd
from typing import Dict, List, Optional, Any
from datetime import datetime
import threading
import time

from config import DATABASE_TYPE, get_database_config
if DATABASE_TYPE.lower() == "sqlite":
    from database_manager_sqlite import DatabaseManager
else:
    from database_manager import DatabaseManager

from logger_utils import process_logger


class DatabaseManagementWindow:
    """数据库管理窗口 - 完全类型安全版本"""
    
    def __init__(self, parent):
        self.parent = parent
        self.db_manager = DatabaseManager(get_database_config())
        
        # 创建窗口
        self.window = tk.Toplevel(parent)
        self.window.title("数据库管理 - RMS系统")
        self.window.geometry("1200x800")
        self.window.resizable(True, True)
        
        # 表格配置
        self.table_configs = {
            "contracts": {
                "name": "合同数据表",
                "table": "contracts",
                "key_fields": ["contractid"],
                "display_fields": [
                    "id", "contractid", "description", "leader", "contractfunds", "contractclassification", 
                    "signdate", "undertakingunit", "projectmembers", "registrationid", "startdate", "enddate", 
                    "leadertype", "leadertelephone", "leaderemail", "operator", "operatortelephone", 
                    "contracteffectivestatus", "contractstatus", "paymentmode", "partyaseal", "partybseal", 
                    "contractrecovered", "statisticalattribution", "subjectclassification", "researchcategory", 
                    "formscooperation", "projectsource", "socialeconomictarget", "neic", "auditstatus", 
                    "remarks", "iseffective", "partyaname", "partyatype", "partyacontact", "partyatel", 
                    "partaprovince", "partyacity", "partaaddress", "partapostalcode", "patentcount", 
                    "amountreceived", "copyrightid", "manageremployeeid", "copyrightcount", "fundids",
                    "purchasefunds", "cooperationfunds",
                    "updateid", "created_at", "updated_at"
                ]
            },
            "projectfunds": {
                "name": "经费数据表", 
                "table": "projectfunds",
                "key_fields": ["fundid", "funds_received", "contractid"],
                "display_fields": [
                    "id", "project_name", "fund_manager", "funds_received", "fundid", "allocation_date", 
                    "project_unit", "contractid", "approved_funds", "manager_id", "project_leader", 
                    "fund_unit", "project_category", "receipt_id", "retained_funds", "allocated_funds", 
                    "payment_unit", "payment_type", "audit_status", "project_nature", "project_level", 
                    "updateid", "created_at", "updated_at"
                ]
            },
            "transactions": {
                "name": "收支明细表",
                "table": "transactions", 
                "key_fields": ["fundid", "transactiondate", "vouchernumber"],
                "display_fields": [
                    "id", "fundid", "transactiondate", "vouchernumber", "summary", "subjectcode", 
                    "subjectname", "debitamount", "creditamount", "balance", "endingbalance", 
                    "totaldebit", "totalcredit", "projectname", "sequencenumber", "updateid", 
                    "created_at", "updated_at"
                ]
            }
        }
        
        # 数据存储
        self.table_data: Dict[str, List[Dict[str, Any]]] = {}
        self.filtered_data: Dict[str, List[Dict[str, Any]]] = {}
        self.auto_refresh = tk.BooleanVar(value=False)
        self.refresh_interval = 5
        self.refresh_thread: Optional[threading.Thread] = None
        self.refresh_running = False
        
        # 分页和搜索相关变量
        self.current_page: Dict[str, int] = {}
        self.items_per_page = 999999  # 显示所有数据，不分页
        self.total_pages: Dict[str, int] = {}
        
        self.setup_ui()
        self.load_all_data()
        
        # 窗口关闭事件
        self.window.protocol("WM_DELETE_WINDOW", self.on_closing)

    def _safe_query_one(self, sql: str, params: Optional[tuple] = None) -> Optional[Dict[str, Any]]:
        """类型安全的单条记录查询"""
        try:
            result = self.db_manager.execute_query(sql, params, fetch_one=True)
            return result if isinstance(result, dict) else None
        except Exception as e:
            process_logger.log_error("数据库查询", f"查询失败: {str(e)}")
            return None

    def _safe_query_list(self, sql: str, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
        """类型安全的多条记录查询"""
        try:
            result = self.db_manager.execute_query(sql, params, fetch_one=False)
            return result if isinstance(result, list) else []
        except Exception as e:
            process_logger.log_error("数据库查询", f"查询失败: {str(e)}")
            return []

    def setup_ui(self):
        """设置用户界面"""
        # 创建主框架
        main_frame = ttk.Frame(self.window)
        main_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # 顶部控制面板
        self.create_control_panel(main_frame)
        
        # 创建Notebook组件用于分页显示表格
        self.notebook = ttk.Notebook(main_frame)
        self.notebook.pack(fill=tk.BOTH, expand=True, pady=(10, 0))
        
        # 为每个表创建一个页面
        self.tree_widgets: Dict[str, ttk.Treeview] = {}
        for table_key, config in self.table_configs.items():
            self.create_table_tab(table_key, config)
        
        # 状态栏
        self.create_status_bar(main_frame)

    def create_control_panel(self, parent):
        """创建控制面板"""
        control_frame = ttk.LabelFrame(parent, text="数据库操作", padding="10")
        control_frame.pack(fill=tk.X, pady=(0, 10))
        
        # 第一行：刷新控制
        row1_frame = ttk.Frame(control_frame)
        row1_frame.pack(fill=tk.X, pady=(0, 5))
        
        ttk.Button(row1_frame, text="🔄 手动刷新", command=self.refresh_all_data, width=15).pack(side=tk.LEFT, padx=(0, 10))
        ttk.Checkbutton(row1_frame, text="自动刷新", variable=self.auto_refresh, command=self.toggle_auto_refresh).pack(side=tk.LEFT, padx=(0, 10))
        
        ttk.Label(row1_frame, text="刷新间隔:").pack(side=tk.LEFT, padx=(0, 5))
        self.interval_var = tk.StringVar(value="5")
        interval_spin = ttk.Spinbox(row1_frame, from_=1, to=60, width=5, textvariable=self.interval_var, command=self.update_refresh_interval)
        interval_spin.pack(side=tk.LEFT, padx=(0, 5))
        ttk.Label(row1_frame, text="秒").pack(side=tk.LEFT)
        
        # 第二行：搜索功能
        row2_frame = ttk.Frame(control_frame)
        row2_frame.pack(fill=tk.X, pady=(5, 0))
        
        ttk.Label(row2_frame, text="🔍 搜索:").pack(side=tk.LEFT, padx=(0, 5))
        self.search_entry = ttk.Entry(row2_frame, width=20)
        self.search_entry.pack(side=tk.LEFT, padx=(0, 10))
        self.search_entry.bind('<KeyRelease>', self.on_search_change)
        
        ttk.Button(row2_frame, text="清除", command=self.clear_search, width=8).pack(side=tk.LEFT, padx=(0, 20))
        
        # 第三行：数据操作
        row3_frame = ttk.Frame(control_frame)
        row3_frame.pack(fill=tk.X, pady=(5, 0))
        
        ttk.Button(row3_frame, text="🗑️ 清空当前表", command=self.clear_current_table, width=15).pack(side=tk.LEFT, padx=(0, 10))
        ttk.Button(row3_frame, text="🗑️ 清空所有表", command=self.clear_all_tables, width=15).pack(side=tk.LEFT, padx=(0, 10))
        ttk.Button(row3_frame, text="📊 导出数据", command=self.export_data, width=15).pack(side=tk.LEFT, padx=(0, 10))
        ttk.Button(row3_frame, text="📈 统计信息", command=self.show_statistics, width=15).pack(side=tk.LEFT, padx=(0, 10))

    def create_table_tab(self, table_key: str, config: Dict):
        """创建表格页面"""
        # 创建页面框架
        tab_frame = ttk.Frame(self.notebook)
        self.notebook.add(tab_frame, text=f"{config['name']}")
        
        # 创建表格框架
        table_frame = ttk.Frame(tab_frame)
        table_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # 创建Treeview组件
        columns = config['display_fields']
        tree = ttk.Treeview(table_frame, columns=columns, show='tree headings', height=20)
        
        # 配置列
        tree.column('#0', width=50, minwidth=50)
        tree.heading('#0', text='序号')
        
        for col in columns:
            tree.column(col, width=120, minwidth=80)
            tree.heading(col, text=col)
        
        # 添加滚动条
        v_scrollbar = ttk.Scrollbar(table_frame, orient=tk.VERTICAL, command=tree.yview)
        h_scrollbar = ttk.Scrollbar(table_frame, orient=tk.HORIZONTAL, command=tree.xview)
        tree.configure(yscrollcommand=v_scrollbar.set, xscrollcommand=h_scrollbar.set)
        
        # 布局
        tree.grid(row=0, column=0, sticky='nsew')
        v_scrollbar.grid(row=0, column=1, sticky='ns')
        h_scrollbar.grid(row=1, column=0, sticky='ew')
        
        table_frame.grid_rowconfigure(0, weight=1)
        table_frame.grid_columnconfigure(0, weight=1)
        
        # 初始化分页变量
        self.current_page[table_key] = 1
        self.total_pages[table_key] = 1
        
        # 保存引用
        self.tree_widgets[table_key] = tree

    def create_status_bar(self, parent):
        """创建状态栏"""
        status_frame = ttk.Frame(parent)
        status_frame.pack(fill=tk.X, pady=(10, 0))
        
        self.status_var = tk.StringVar(value="就绪")
        self.record_counts_var = tk.StringVar(value="")
        
        ttk.Label(status_frame, textvariable=self.status_var).pack(side=tk.LEFT)
        ttk.Label(status_frame, textvariable=self.record_counts_var).pack(side=tk.RIGHT)

    def load_table_data(self, table_key: str):
        """加载指定表的数据"""
        try:
            config = self.table_configs[table_key]
            table_name = config['table']
            
            # 查询数据
            sql = f"SELECT * FROM {table_name} ORDER BY id DESC"
            records = self._safe_query_list(sql)
            
            # 保存数据
            self.table_data[table_key] = records
            self.filtered_data[table_key] = records
            
            # 重置分页
            self.current_page[table_key] = 1
            
            # 更新树形视图
            self.update_tree_view(table_key)
            
            process_logger.logger.info(f"加载{config['name']}数据: {len(self.table_data[table_key])}条记录")
            
        except Exception as e:
            error_msg = f"加载{table_key}表数据失败: {str(e)}"
            process_logger.log_error("数据库管理", error_msg)
            messagebox.showerror("错误", error_msg)

    def update_tree_view(self, table_key: str):
        """更新树形视图显示"""
        tree = self.tree_widgets[table_key]
        config = self.table_configs[table_key]
        
        # 清空现有数据
        for item in tree.get_children():
            tree.delete(item)
        
        # 获取分页数据
        page_data = self.get_paginated_data(table_key)
        
        # 计算起始序号
        current_page = self.current_page.get(table_key, 1)
        start_index = (current_page - 1) * self.items_per_page
        
        # 添加新数据
        for i, record in enumerate(page_data):
            values = []
            for field in config['display_fields']:
                value = record.get(field, '')
                if value is None:
                    value = ''
                elif isinstance(value, (int, float)):
                    value = str(value)
                elif len(str(value)) > 50:
                    value = str(value)[:47] + '...'
                values.append(value)
            
            tree.insert('', 'end', text=str(start_index + i + 1), values=values)

    def get_paginated_data(self, table_key: str) -> List[Dict[str, Any]]:
        """获取当前页的数据"""
        filtered_records = self.filtered_data.get(table_key, [])
        
        page = self.current_page.get(table_key, 1)
        start_index = (page - 1) * self.items_per_page
        end_index = start_index + self.items_per_page
        
        return filtered_records[start_index:end_index]

    def load_all_data(self):
        """加载所有表的数据"""
        self.status_var.set("正在加载数据...")
        self.window.update()
        
        try:
            for table_key in self.table_configs.keys():
                self.load_table_data(table_key)
            
            self.update_status()
            self.status_var.set("数据加载完成")
            
        except Exception as e:
            error_msg = f"加载数据失败: {str(e)}"
            process_logger.log_error("数据库管理", error_msg)
            self.status_var.set(f"错误: {error_msg}")

    def refresh_all_data(self):
        """刷新所有数据"""
        self.load_all_data()

    def update_status(self):
        """更新状态信息"""
        counts = []
        for table_key, config in self.table_configs.items():
            count = len(self.table_data.get(table_key, []))
            counts.append(f"{config['name']}: {count}")
        
        self.record_counts_var.set(" | ".join(counts))

    def clear_current_table(self):
        """清空当前选中的表"""
        current_tab = self.notebook.index(self.notebook.select())
        table_keys = list(self.table_configs.keys())
        
        if current_tab < len(table_keys):
            table_key = table_keys[current_tab]
            config = self.table_configs[table_key]
            
            result = messagebox.askyesno(
                "确认清空",
                f"确定要清空 {config['name']} 的所有数据吗？\n\n此操作不可恢复！",
                icon="warning"
            )
            
            if result:
                self.clear_table(table_key)

    def clear_all_tables(self):
        """清空所有表"""
        result = messagebox.askyesno(
            "确认清空",
            "确定要清空所有数据表吗？\n\n包括：\n- 合同数据表\n- 经费数据表\n- 收支明细表\n\n此操作不可恢复！",
            icon="warning"
        )
        
        if result:
            confirm = messagebox.askyesno(
                "最终确认",
                "这是最后一次确认。\n\n清空后所有数据将永久丢失！\n\n确定继续吗？",
                icon="warning"
            )
            
            if confirm:
                for table_key in self.table_configs.keys():
                    self.clear_table(table_key)

    def clear_table(self, table_key: str):
        """清空指定表"""
        try:
            config = self.table_configs[table_key]
            table_name = config['table']
            
            self.status_var.set(f"正在清空{config['name']}...")
            self.window.update()
            
            # 执行删除
            sql = f"DELETE FROM {table_name}"
            affected_rows = self.db_manager.execute_update(sql)
            
            # 重置自增ID（SQLite）
            if DATABASE_TYPE.lower() == "sqlite":
                reset_sql = f"UPDATE sqlite_sequence SET seq = 0 WHERE name = '{table_name}'"
                self.db_manager.execute_update(reset_sql)
            
            # 刷新显示
            self.load_table_data(table_key)
            
            process_logger.logger.info(f"清空{config['name']}完成，删除{affected_rows}条记录")
            self.status_var.set(f"{config['name']}已清空")
            
            messagebox.showinfo("成功", f"{config['name']}已清空\n删除了 {affected_rows} 条记录")
            
        except Exception as e:
            error_msg = f"清空{config['name']}失败: {str(e)}"
            process_logger.log_error("数据库管理", error_msg)
            self.status_var.set(f"错误: {error_msg}")
            messagebox.showerror("错误", error_msg)

    def export_data(self):
        """导出数据"""
        current_tab = self.notebook.index(self.notebook.select())
        table_keys = list(self.table_configs.keys())
        
        if current_tab < len(table_keys):
            table_key = table_keys[current_tab]
            config = self.table_configs[table_key]
            
            filename = filedialog.asksaveasfilename(
                title=f"导出{config['name']}",
                defaultextension=".xlsx",
                filetypes=[("Excel文件", "*.xlsx"), ("CSV文件", "*.csv"), ("所有文件", "*.*")]
            )
            
            if filename:
                try:
                    records = self.table_data.get(table_key, [])
                    if not records:
                        messagebox.showwarning("警告", "没有数据可导出")
                        return
                    
                    df = pd.DataFrame(records)
                    
                    if filename.endswith('.xlsx'):
                        df.to_excel(filename, index=False)
                    else:
                        df.to_csv(filename, index=False, encoding='utf-8-sig')
                    
                    messagebox.showinfo("成功", f"数据已导出到: {filename}")
                    
                except Exception as e:
                    error_msg = f"导出失败: {str(e)}"
                    messagebox.showerror("错误", error_msg)

    def show_statistics(self):
        """显示统计信息"""
        try:
            stats_window = tk.Toplevel(self.window)
            stats_window.title("数据库统计分析")
            stats_window.geometry("600x500")
            stats_window.resizable(True, True)
            
            text_widget = tk.Text(stats_window, wrap=tk.WORD, font=('Courier', 10))
            scrollbar = ttk.Scrollbar(stats_window, orient=tk.VERTICAL, command=text_widget.yview)
            text_widget.configure(yscrollcommand=scrollbar.set)
            
            # 生成统计内容
            stats_content = f"{'='*50}\n"
            stats_content += f"数据库统计报告\n"
            stats_content += f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
            stats_content += f"{'='*50}\n\n"
            
            total_records = 0
            for table_key, config in self.table_configs.items():
                table_name = config['table']
                
                # 安全的记录数查询
                count_result = self._safe_query_one(f"SELECT COUNT(*) as count FROM {table_name}")
                record_count = count_result.get('count', 0) if count_result else 0
                total_records += record_count
                
                stats_content += f"{config['name']} ({table_name}):\n"
                stats_content += f"  总记录数: {record_count:,}\n"
                
                if record_count > 0:
                    # 时间统计
                    time_result = self._safe_query_one(f"SELECT MIN(created_at) as earliest, MAX(created_at) as latest FROM {table_name}")
                    if time_result:
                        earliest = time_result.get('earliest', '无数据')
                        latest = time_result.get('latest', '无数据')
                        stats_content += f"  最早记录: {earliest}\n"
                        stats_content += f"  最新记录: {latest}\n"
                
                stats_content += "\n"
            
            stats_content += f"总记录数: {total_records:,}\n"
            
            text_widget.insert('1.0', stats_content)
            text_widget.config(state=tk.DISABLED)
            
            text_widget.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=10, pady=10)
            scrollbar.pack(side=tk.RIGHT, fill=tk.Y, pady=10)
            
        except Exception as e:
            error_msg = f"生成统计信息失败: {str(e)}"
            process_logger.log_error("统计分析", error_msg)
            messagebox.showerror("错误", error_msg)

    def on_search_change(self, event=None):
        """搜索内容变化时的处理"""
        search_text = self.search_entry.get().strip().lower()
        current_tab = self.notebook.index(self.notebook.select())
        table_keys = list(self.table_configs.keys())
        
        if current_tab < len(table_keys):
            table_key = table_keys[current_tab]
            self.filter_data(table_key, search_text)
            self.update_tree_view(table_key)

    def clear_search(self):
        """清除搜索条件"""
        self.search_entry.delete(0, tk.END)
        current_tab = self.notebook.index(self.notebook.select())
        table_keys = list(self.table_configs.keys())
        
        if current_tab < len(table_keys):
            table_key = table_keys[current_tab]
            self.filtered_data[table_key] = self.table_data.get(table_key, [])
            self.current_page[table_key] = 1
            self.update_tree_view(table_key)

    def filter_data(self, table_key: str, search_text: str):
        """过滤数据"""
        if not search_text:
            self.filtered_data[table_key] = self.table_data.get(table_key, [])
        else:
            records = self.table_data.get(table_key, [])
            filtered_records = []
            
            for record in records:
                match_found = False
                for key, value in record.items():
                    if value and search_text in str(value).lower():
                        match_found = True
                        break
                
                if match_found:
                    filtered_records.append(record)
            
            self.filtered_data[table_key] = filtered_records
        
        self.current_page[table_key] = 1

    def toggle_auto_refresh(self):
        """切换自动刷新"""
        if self.auto_refresh.get():
            self.start_auto_refresh()
        else:
            self.stop_auto_refresh()

    def update_refresh_interval(self):
        """更新刷新间隔"""
        try:
            self.refresh_interval = int(self.interval_var.get())
        except ValueError:
            self.refresh_interval = 5
            self.interval_var.set("5")

    def start_auto_refresh(self):
        """开始自动刷新"""
        if not self.refresh_running:
            self.refresh_running = True
            self.refresh_thread = threading.Thread(target=self.auto_refresh_worker, daemon=True)
            self.refresh_thread.start()

    def stop_auto_refresh(self):
        """停止自动刷新"""
        self.refresh_running = False

    def auto_refresh_worker(self):
        """自动刷新工作线程"""
        while self.refresh_running:
            time.sleep(self.refresh_interval)
            if self.refresh_running and self.auto_refresh.get():
                self.window.after(0, self.refresh_all_data)
            elif not self.auto_refresh.get():
                break

    def on_closing(self):
        """窗口关闭事件"""
        self.stop_auto_refresh()
        if self.db_manager:
            self.db_manager.close_connection()
        self.window.destroy()


def show_database_management(parent):
    """显示数据库管理窗口"""
    DatabaseManagementWindow(parent) 