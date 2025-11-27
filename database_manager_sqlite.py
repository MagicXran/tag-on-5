"""
SQLite数据库管理模块 - 负责SQLite数据库的连接、读取、插入和更新操作
"""

import sqlite3
import pandas as pd
import os
from typing import Dict, List, Tuple, Optional, Any, Union
from logger_utils import process_logger


class DatabaseManager:
    """SQLite数据库管理器，负责SQLite数据库操作"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        # SQLite配置简化为数据库文件路径
        self.config = config or {"database": "rms.db"}
        self.connection = None
        self.db_path = self.config.get("database", "rms.db")
        
        # 确保数据库文件所在目录存在
        db_dir = os.path.dirname(self.db_path)
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir)
        
        # 初始化数据库表结构
        self._init_database()

    def _init_database(self):
        """初始化数据库表结构"""
        conn = self.get_connection()
        try:
            with conn:
                # 创建合同表
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS contracts (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        contractid TEXT NOT NULL,
                        description TEXT,
                        leader TEXT,
                        contractfunds REAL,
                        contractclassification TEXT,
                        signdate TEXT,
                        undertakingunit TEXT,
                        projectmembers TEXT,
                        registrationid TEXT,
                        startdate TEXT,
                        enddate TEXT,
                        leadertype TEXT,
                        leadertelephone TEXT,
                        leaderemail TEXT,
                        operator TEXT,
                        operatortelephone TEXT,
                        contracteffectivestatus TEXT,
                        contractstatus TEXT,
                        paymentmode TEXT,
                        partyaseal TEXT,
                        partybseal TEXT,
                        contractrecovered TEXT,
                        statisticalattribution TEXT,
                        subjectclassification TEXT,
                        researchcategory TEXT,
                        formscooperation TEXT,
                        projectsource TEXT,
                        socialeconomictarget TEXT,
                        neic TEXT,
                        auditstatus TEXT,
                        remarks TEXT,
                        iseffective TEXT,
                        partyaname TEXT,
                        partyatype TEXT,
                        partyacontact TEXT,
                        partyatel TEXT,
                        partaprovince TEXT,
                        partyacity TEXT,
                        partaaddress TEXT,
                        partapostalcode TEXT,
                        patentcount INTEGER,
                        amountreceived REAL,
                        copyrightid TEXT,
                        manageremployeeid TEXT,
                        copyrightcount INTEGER,
                        fundids TEXT,
                        conversiontype TEXT,
                        licensetype TEXT,
                        effective_date TEXT,
                        department TEXT,
                        updateid VARCHAR(100),
                        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                        updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(contractid, fundids)
                    )
                """)
                
                # 创建项目资金表
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS projectfunds (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        unid VARCHAR(100) NOT NULL UNIQUE,
                        project_name TEXT,
                        fund_manager TEXT,
                        funds_received REAL,
                        fundid TEXT,
                        allocation_date TEXT,
                        project_unit TEXT,
                        contractid TEXT,
                        approved_funds REAL,
                        manager_id TEXT,
                        project_leader TEXT,
                        fund_unit TEXT,
                        project_category TEXT,
                        receipt_id TEXT,
                        retained_funds REAL,
                        allocated_funds REAL,
                        payment_unit TEXT,
                        payment_type TEXT,
                        audit_status TEXT,
                        project_nature TEXT,
                        project_level TEXT,
                        updateid VARCHAR(100),
                        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                        updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                # 创建收支明细表
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS transactions (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        fundid TEXT,
                        transactiondate TEXT,
                        vouchernumber TEXT,
                        summary TEXT,
                        subjectcode TEXT,
                        subjectname TEXT,
                        debitamount REAL,
                        creditamount REAL,
                        balance REAL,
                        endingbalance REAL,
                        totaldebit REAL,
                        totalcredit REAL,
                        openingbalance REAL,
                        projectname TEXT,
                        sequencenumber INTEGER,
                        updateid VARCHAR(100),
                        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                        updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(fundid, transactiondate, vouchernumber, debitamount, balance)
                    )
                """)
                
                # 为现有表添加updateid字段（如果不存在）
                try:
                    conn.execute("ALTER TABLE contracts ADD COLUMN updateid VARCHAR(100)")
                except sqlite3.OperationalError:
                    pass  # 字段已存在
                
                # 为现有表添加department字段（如果不存在）
                try:
                    conn.execute("ALTER TABLE contracts ADD COLUMN department TEXT")
                except sqlite3.OperationalError:
                    pass  # 字段已存在
                
                try:
                    conn.execute("ALTER TABLE projectfunds ADD COLUMN updateid VARCHAR(100)")
                except sqlite3.OperationalError:
                    pass  # 字段已存在

                # 为现有projectfunds表添加unid字段（如果不存在）
                try:
                    conn.execute("ALTER TABLE projectfunds ADD COLUMN unid VARCHAR(100)")
                    # 为现有记录生成unid（使用id字段拼接时间戳作为临时方案）
                    import uuid
                    cursor = conn.cursor()
                    cursor.execute("SELECT id FROM projectfunds WHERE unid IS NULL OR unid = ''")
                    rows = cursor.fetchall()
                    for row in rows:
                        new_unid = str(uuid.uuid4())
                        conn.execute("UPDATE projectfunds SET unid = ? WHERE id = ?", (new_unid, row[0]))
                    if rows:
                        process_logger.logger.info(f"为 {len(rows)} 条现有经费记录生成了unid")
                except sqlite3.OperationalError:
                    pass  # 字段已存在
                
                try:
                    conn.execute("ALTER TABLE transactions ADD COLUMN updateid VARCHAR(100)")
                except sqlite3.OperationalError:
                    pass  # 字段已存在

                # 为现有transactions表添加openingbalance字段（如果不存在）
                try:
                    conn.execute("ALTER TABLE transactions ADD COLUMN openingbalance REAL")
                except sqlite3.OperationalError:
                    pass  # 字段已存在

                # 如果存在oa_sync_id字段，将其数据迁移到updateid字段
                try:
                    conn.execute("UPDATE contracts SET updateid = oa_sync_id WHERE oa_sync_id IS NOT NULL")
                    conn.execute("UPDATE projectfunds SET updateid = oa_sync_id WHERE oa_sync_id IS NOT NULL")
                    conn.execute("UPDATE transactions SET updateid = oa_sync_id WHERE oa_sync_id IS NOT NULL")
                except sqlite3.OperationalError:
                    pass  # oa_sync_id字段不存在
                
                # 检查并升级transactions表的唯一约束
                self._upgrade_transactions_table_if_needed(conn)

                # 检查并升级projectfunds表（移除旧联合主键，改为unid主键）
                self._upgrade_projectfunds_table_if_needed(conn)

                # 启用WAL模式提高并发性能
                conn.execute("PRAGMA journal_mode=WAL")
                # 启用外键约束
                conn.execute("PRAGMA foreign_keys=ON")
                
                process_logger.logger.info("数据库初始化完成")
                
        except Exception as e:
            process_logger.log_error("数据库初始化", str(e))
            raise
    
    def _upgrade_transactions_table_if_needed(self, conn):
        """检查并升级transactions表的唯一约束"""
        try:
            # 获取当前表的索引信息
            cursor = conn.cursor()
            cursor.execute("PRAGMA index_list(transactions)")
            indexes = cursor.fetchall()
            
            # 检查是否存在旧的唯一约束（只有3个字段）
            needs_upgrade = True
            for index in indexes:
                if index[2] == 1:  # unique index
                    cursor.execute(f"PRAGMA index_info({index[1]})")
                    index_cols = cursor.fetchall()
                    if len(index_cols) == 5:  # 新的唯一约束有5个字段
                        needs_upgrade = False
                        break
            
            if needs_upgrade:
                process_logger.logger.info("检测到transactions表需要升级唯一约束")
                
                # 备份现有数据
                cursor.execute("SELECT * FROM transactions")
                existing_data = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
                
                # 删除旧表
                conn.execute("DROP TABLE IF EXISTS transactions_backup")
                conn.execute("ALTER TABLE transactions RENAME TO transactions_backup")
                
                # 创建新表（包含新的唯一约束）
                conn.execute("""
                    CREATE TABLE transactions (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        fundid TEXT,
                        transactiondate TEXT,
                        vouchernumber TEXT,
                        summary TEXT,
                        subjectcode TEXT,
                        subjectname TEXT,
                        debitamount REAL,
                        creditamount REAL,
                        balance REAL,
                        endingbalance REAL,
                        totaldebit REAL,
                        totalcredit REAL,
                        projectname TEXT,
                        sequencenumber INTEGER,
                        updateid VARCHAR(100),
                        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                        updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(fundid, transactiondate, vouchernumber, debitamount, balance)
                    )
                """)
                
                # 恢复数据
                if existing_data:
                    # 构建插入语句
                    placeholders = ','.join(['?' for _ in columns])
                    insert_sql = f"INSERT INTO transactions ({','.join(columns)}) VALUES ({placeholders})"
                    
                    # 批量插入数据
                    for row in existing_data:
                        try:
                            conn.execute(insert_sql, row)
                        except sqlite3.IntegrityError as e:
                            # 如果有重复数据，记录日志但继续处理
                            process_logger.log_warning("数据迁移", f"跳过重复记录: {str(e)}")
                
                # 删除备份表
                conn.execute("DROP TABLE transactions_backup")
                
                process_logger.logger.info("transactions表唯一约束升级完成")
                
        except Exception as e:
            process_logger.log_error("升级transactions表", str(e))
            # 如果升级失败，尝试恢复
            try:
                conn.execute("DROP TABLE IF EXISTS transactions")
                conn.execute("ALTER TABLE transactions_backup RENAME TO transactions")
            except:
                pass
            raise

    def _upgrade_projectfunds_table_if_needed(self, conn):
        """检查并升级projectfunds表，移除旧的联合唯一约束，改为unid主键"""
        import uuid

        try:
            cursor = conn.cursor()

            # 检查表是否存在
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='projectfunds'")
            if not cursor.fetchone():
                return  # 表不存在，无需升级

            # 检查是否存在旧的唯一约束（基于fundid, allocation_date, contractid）
            cursor.execute("PRAGMA index_list(projectfunds)")
            indexes = cursor.fetchall()

            has_old_constraint = False
            for index in indexes:
                if index[2] == 1:  # unique index
                    cursor.execute(f"PRAGMA index_info({index[1]})")
                    index_cols = cursor.fetchall()
                    col_names = [col[2] for col in index_cols]
                    # 检查是否是旧的联合唯一约束
                    if 'fundid' in col_names and 'allocation_date' in col_names and 'contractid' in col_names:
                        has_old_constraint = True
                        break

            if not has_old_constraint:
                # 检查unid字段是否已有唯一约束
                for index in indexes:
                    if index[2] == 1:  # unique index
                        cursor.execute(f"PRAGMA index_info({index[1]})")
                        index_cols = cursor.fetchall()
                        if len(index_cols) == 1 and index_cols[0][2] == 'unid':
                            process_logger.logger.info("projectfunds表已是新结构，无需升级")
                            return
                return  # 没有旧约束，无需升级

            process_logger.logger.info("检测到projectfunds表需要升级：移除旧联合唯一约束，改为unid主键")

            # 备份现有数据
            cursor.execute("SELECT * FROM projectfunds")
            existing_data = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]

            # 检查unid列是否存在
            has_unid = 'unid' in columns

            # 删除旧表，创建新表
            conn.execute("DROP TABLE IF EXISTS projectfunds_backup")
            conn.execute("ALTER TABLE projectfunds RENAME TO projectfunds_backup")

            # 创建新表（unid为唯一主键）
            conn.execute("""
                CREATE TABLE projectfunds (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    unid VARCHAR(100) NOT NULL UNIQUE,
                    project_name TEXT,
                    fund_manager TEXT,
                    funds_received REAL,
                    fundid TEXT,
                    allocation_date TEXT,
                    project_unit TEXT,
                    contractid TEXT,
                    approved_funds REAL,
                    manager_id TEXT,
                    project_leader TEXT,
                    fund_unit TEXT,
                    project_category TEXT,
                    receipt_id TEXT,
                    retained_funds REAL,
                    allocated_funds REAL,
                    payment_unit TEXT,
                    payment_type TEXT,
                    audit_status TEXT,
                    project_nature TEXT,
                    project_level TEXT,
                    updateid VARCHAR(100),
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # 恢复数据
            if existing_data:
                # 构建新表的列名列表
                new_columns = ['id', 'unid', 'project_name', 'fund_manager', 'funds_received',
                               'fundid', 'allocation_date', 'project_unit', 'contractid',
                               'approved_funds', 'manager_id', 'project_leader', 'fund_unit',
                               'project_category', 'receipt_id', 'retained_funds', 'allocated_funds',
                               'payment_unit', 'payment_type', 'audit_status', 'project_nature',
                               'project_level', 'updateid', 'created_at', 'updated_at']

                for row in existing_data:
                    row_dict = dict(zip(columns, row))

                    # 如果没有unid或unid为空，生成新的UUID
                    if not has_unid or not row_dict.get('unid'):
                        row_dict['unid'] = str(uuid.uuid4())

                    # 构建插入数据
                    insert_cols = []
                    insert_vals = []
                    for col in new_columns:
                        if col in row_dict:
                            insert_cols.append(col)
                            insert_vals.append(row_dict[col])

                    placeholders = ','.join(['?' for _ in insert_cols])
                    insert_sql = f"INSERT INTO projectfunds ({','.join(insert_cols)}) VALUES ({placeholders})"

                    try:
                        conn.execute(insert_sql, insert_vals)
                    except sqlite3.IntegrityError as e:
                        process_logger.log_warning("数据迁移", f"跳过重复记录: {str(e)}")

            # 删除备份表
            conn.execute("DROP TABLE projectfunds_backup")

            process_logger.logger.info(f"projectfunds表升级完成，迁移了 {len(existing_data)} 条记录")

        except Exception as e:
            process_logger.log_error("升级projectfunds表", str(e))
            # 如果升级失败，尝试恢复
            try:
                conn.execute("DROP TABLE IF EXISTS projectfunds")
                conn.execute("ALTER TABLE projectfunds_backup RENAME TO projectfunds")
            except:
                pass
            raise

    def validate_config(self) -> Tuple[bool, str]:
        """验证数据库配置
        
        Returns:
            Tuple[bool, str]: (配置是否有效, 详细信息)
        """
        try:
            if not self.db_path:
                return False, "数据库文件路径未配置"
            
            # 检查数据库文件是否可以创建/访问
            db_dir = os.path.dirname(self.db_path)
            if db_dir and not os.path.exists(db_dir):
                try:
                    os.makedirs(db_dir)
                except Exception as e:
                    return False, f"无法创建数据库目录: {str(e)}"
            
            # 尝试连接数据库
            test_conn = sqlite3.connect(self.db_path)
            test_conn.close()
            
            return True, "配置验证通过"
            
        except Exception as e:
            return False, f"配置验证失败: {str(e)}"

    def get_connection_diagnostic_info(self) -> Dict[str, Any]:
        """获取连接诊断信息
        
        Returns:
            Dict[str, Any]: 诊断信息
        """
        diagnostic_info = {
            "config_valid": False,
            "config_message": "",
            "connection_test": False,
            "connection_message": "",
            "sqlite_version": "",
            "database_path": self.db_path,
            "database_exists": False,
            "database_size": 0,
            "detailed_error": ""
        }
        
        try:
            # 获取SQLite版本
            diagnostic_info["sqlite_version"] = sqlite3.sqlite_version
            
            # 验证配置
            config_valid, config_message = self.validate_config()
            diagnostic_info["config_valid"] = config_valid
            diagnostic_info["config_message"] = config_message
            
            if not config_valid:
                return diagnostic_info
            
            # 检查数据库文件
            if os.path.exists(self.db_path):
                diagnostic_info["database_exists"] = True
                diagnostic_info["database_size"] = os.path.getsize(self.db_path)
            
            # 测试连接
            connection_success, connection_message = self.test_connection()
            diagnostic_info["connection_test"] = connection_success
            diagnostic_info["connection_message"] = connection_message
            
        except Exception as e:
            diagnostic_info["detailed_error"] = str(e)
            
        return diagnostic_info

    def refresh_connection(self):
        """刷新数据库连接"""
        try:
            if self.connection:
                self.connection.close()
                self.connection = None
                process_logger.logger.info("数据库连接 - 已关闭旧连接")
            
            # 重新建立连接
            self.get_connection()
            process_logger.logger.info("数据库连接 - 连接已刷新")
            
        except Exception as e:
            error_msg = f"刷新连接失败: {str(e)}"
            process_logger.log_error("数据库连接", error_msg)
            raise Exception(error_msg) from e
    
    def get_connection(self):
        """获取数据库连接"""
        if self.connection is None:
            try:
                process_logger.logger.info(f"数据库连接 - 尝试连接到 {self.db_path}")
                
                self.connection = sqlite3.connect(
                    self.db_path,
                    timeout=30,  # 30秒超时
                    check_same_thread=False,  # 允许多线程访问
                    isolation_level=None  # 自动提交模式
                )
                
                # 设置行工厂，使查询结果为字典格式
                self.connection.row_factory = sqlite3.Row
                
                process_logger.logger.info("数据库连接 - 连接建立成功")
                
            except Exception as e:
                error_msg = f"数据库连接失败: {str(e)}"
                process_logger.log_error("数据库连接", error_msg)
                raise Exception(error_msg) from e
                
        return self.connection
    
    def test_connection(self) -> Tuple[bool, str]:
        """测试数据库连接
        
        Returns:
            Tuple[bool, str]: (连接是否成功, 详细信息)
        """
        try:
            conn = self.get_connection()
            if not conn:
                return False, "无法建立数据库连接"
            
            # 执行简单查询测试
            cursor = conn.cursor()
            cursor.execute("SELECT 1 as test_value")
            result = cursor.fetchone()
            if result and result['test_value'] == 1:
                return True, "数据库连接成功"
            else:
                return False, "数据库查询返回异常结果"
                
        except Exception as e:
            error_msg = f"数据库连接测试失败: {str(e)}"
            process_logger.log_error("数据库连接测试", error_msg)
            return False, error_msg
    
    def close_connection(self):
        """关闭数据库连接"""
        if self.connection:
            self.connection.close()
            self.connection = None
    
    def execute_query(self, sql: str, params: Optional[tuple] = None, fetch_one: bool = False, fetch_all: bool = True):
        """执行查询语句
        
        Args:
            sql: SQL语句
            params: 参数
            fetch_one: 是否只获取一条记录
            fetch_all: 是否获取所有记录
            
        Returns:
            查询结果
        """
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(sql, params or ())
            if fetch_one:
                result = cursor.fetchone()
                return dict(result) if result else None
            elif fetch_all:
                results = cursor.fetchall()
                return [dict(row) for row in results]
            else:
                return cursor.rowcount
        except Exception as e:
            process_logger.log_error("数据库查询", str(e), sql=sql)
            raise
    
    def execute_update(self, sql: str, params: Optional[tuple] = None):
        """执行更新语句
        
        Args:
            sql: SQL语句
            params: 参数
            
        Returns:
            影响的行数
        """
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(sql, params or ())
            conn.commit()
            return cursor.rowcount
        except Exception as e:
            conn.rollback()
            process_logger.log_error("数据库更新", str(e), sql=sql)
            raise
    
    def execute_batch_update(self, sql: str, params_list: List[tuple]):
        """执行批量更新语句
        
        Args:
            sql: SQL语句
            params_list: 参数列表
            
        Returns:
            影响的行数
        """
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            cursor.executemany(sql, params_list)
            conn.commit()
            return cursor.rowcount
        except Exception as e:
            conn.rollback()
            process_logger.log_error("数据库批量更新", str(e), sql=sql)
            raise
    
    def check_record_exists(self, table_name: str, primary_keys: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """检查记录是否存在
        
        Args:
            table_name: 表名
            primary_keys: 主键字段和值
            
        Returns:
            如果存在返回记录，否则返回None
        """
        where_clause = " AND ".join([f"{k} = ?" for k in primary_keys.keys()])
        sql = f"SELECT * FROM {table_name} WHERE {where_clause}"
        params = tuple(primary_keys.values())
        
        result = self.execute_query(sql, params, fetch_one=True)
        if isinstance(result, dict):
            return result
        return None
    
    def insert_record(self, table_name: str, data: Dict[str, Any]) -> int:
        """插入记录
        
        Args:
            table_name: 表名
            data: 数据字典
            
        Returns:
            插入的记录ID
        """
        columns = list(data.keys())
        placeholders = ", ".join(["?" for _ in columns])
        sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
        params = tuple(data.values())
        
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(sql, params)
            conn.commit()
            last_row_id = cursor.lastrowid
            return last_row_id if last_row_id is not None else 0
        except Exception as e:
            conn.rollback()
            process_logger.log_error("数据库插入", str(e), sql=sql)
            raise
    
    def update_record(self, table_name: str, data: Dict[str, Any], primary_keys: Dict[str, Any]) -> int:
        """更新记录
        
        Args:
            table_name: 表名
            data: 更新数据
            primary_keys: 主键字段和值
            
        Returns:
            影响的行数
        """
        set_clause = ", ".join([f"{k} = ?" for k in data.keys()])
        where_clause = " AND ".join([f"{k} = ?" for k in primary_keys.keys()])
        sql = f"UPDATE {table_name} SET {set_clause} WHERE {where_clause}"
        params = tuple(data.values()) + tuple(primary_keys.values())
        
        return self.execute_update(sql, params)
    
    def upsert_records(self, table_name: str, records: List[Dict[str, Any]], key_fields: List[str]) -> Dict[str, Any]:
        """批量插入或更新记录
        
        Args:
            table_name: 表名
            records: 记录列表
            key_fields: 主键字段列表
            
        Returns:
            操作结果统计
        """
        if not records:
            return {"inserted": 0, "updated": 0, "total": 0}
        
        inserted_count = 0
        updated_count = 0
        inserted_records = []
        updated_records = []
        
        for record in records:
            # 构建主键条件
            primary_keys = {k: record[k] for k in key_fields if k in record}
            
            # 检查记录是否存在
            existing_record = self.check_record_exists(table_name, primary_keys)
            
            if existing_record:
                # 更新记录
                update_data = {k: v for k, v in record.items() if k not in key_fields}
                if update_data:  # 只有当有非主键字段需要更新时才执行更新
                    update_data['updated_at'] = 'CURRENT_TIMESTAMP'
                    affected_rows = self.update_record(table_name, update_data, primary_keys)
                    if affected_rows > 0:
                        updated_count += 1
                        updated_records.append(record)
            else:
                # 插入记录
                insert_data = record.copy()
                insert_data['created_at'] = 'CURRENT_TIMESTAMP'
                insert_data['updated_at'] = 'CURRENT_TIMESTAMP'
                record_id = self.insert_record(table_name, insert_data)
                if record_id:
                    inserted_count += 1
                    inserted_records.append(record)
        
        return {
            "inserted": inserted_count,
            "updated": updated_count,
            "total": len(records),
            "inserted_records": inserted_records,
            "updated_records": updated_records
        }
    
    def save_contracts_data(self, df: pd.DataFrame) -> Tuple[List[Dict], List[Dict]]:
        """保存合同数据到数据库
        
        Args:
            df: 合同数据DataFrame
            
        Returns:
            Tuple[List[Dict], List[Dict]]: (插入的记录, 更新的记录)
        """
        process_logger.log_start("保存合同数据到数据库")
        
        inserted_records = []
        updated_records = []
        
        try:
            for index, row in df.iterrows():
                # 准备数据（排除空值和NaN）
                data = {}
                for col, value in row.items():
                    if pd.notna(value) and value != "":
                        # 处理datetime类型
                        if hasattr(value, 'strftime'):
                            data[col] = value.strftime('%Y-%m-%d %H:%M:%S')
                        else:
                            data[col] = value
                
                # 主键是contractid和fundids的组合
                primary_key = {
                    "contractid": data.get("contractid"),
                    "fundids": data.get("fundids", "")
                }
                
                # 检查记录是否存在
                existing_record = self.check_record_exists("contracts", primary_key)
                
                if existing_record:
                    # 更新记录
                    update_data = data.copy()
                    # 移除主键字段，避免在UPDATE语句中更新主键
                    update_data.pop("contractid", None)
                    update_data.pop("fundids", None)  # fundids现在也是主键的一部分
                    update_data['updated_at'] = 'CURRENT_TIMESTAMP'
                    
                    if update_data:  # 确保有数据需要更新
                        self.update_record("contracts", update_data, primary_key)
                        
                        # 记录更新信息（包含updateid用于OA同步）
                        record_info = {
                            "operation": "update",
                            "contractid": data.get("contractid"),
                            "updateid": existing_record.get("updateid"),
                            "data": data
                        }
                        updated_records.append(record_info)
                else:
                    # 插入新记录
                    insert_data = data.copy()
                    insert_data['created_at'] = 'CURRENT_TIMESTAMP'
                    insert_data['updated_at'] = 'CURRENT_TIMESTAMP'
                    record_id = self.insert_record("contracts", insert_data)
                    
                    # 记录插入信息
                    record_info = {
                        "operation": "insert",
                        "contractid": data.get("contractid"),
                        "record_id": record_id,
                        "data": data
                    }
                    inserted_records.append(record_info)
            
            process_logger.log_end(
                "保存合同数据到数据库",
                插入记录数=len(inserted_records),
                更新记录数=len(updated_records)
            )
            
            return inserted_records, updated_records
            
        except Exception as e:
            process_logger.log_error("保存合同数据", str(e))
            raise
    
    def save_project_funds_data(self, df: pd.DataFrame) -> Tuple[List[Dict], List[Dict]]:
        """保存项目资金数据到数据库

        经费到账数据永远新增，不更新。每条记录使用系统生成的UUID作为唯一标识（unid）。

        Args:
            df: 项目资金数据DataFrame（必须包含unid字段）

        Returns:
            Tuple[List[Dict], List[Dict]]: (插入的记录, 更新的记录)
            注意：经费数据永远只有插入，updated_records始终为空列表
        """
        process_logger.log_start("保存项目资金数据到数据库")

        inserted_records = []
        updated_records = []  # 经费数据永远不更新，保持空列表以兼容接口

        try:
            for index, row in df.iterrows():
                # 准备数据（排除空值和NaN）
                data = {}
                for col, value in row.items():
                    if pd.notna(value) and value != "":
                        # 处理datetime类型
                        if hasattr(value, 'strftime'):
                            data[col] = value.strftime('%Y-%m-%d %H:%M:%S')
                        else:
                            data[col] = value

                # 确保unid字段存在（应该在data_cleaner中已生成）
                if 'unid' not in data or not data['unid']:
                    import uuid
                    data['unid'] = str(uuid.uuid4())
                    process_logger.log_warning("经费数据", f"记录缺少unid，已自动生成: {data['unid']}")

                # 经费数据永远新增，直接插入
                insert_data = data.copy()
                insert_data['created_at'] = 'CURRENT_TIMESTAMP'
                insert_data['updated_at'] = 'CURRENT_TIMESTAMP'
                record_id = self.insert_record("projectfunds", insert_data)

                # 记录插入信息
                record_info = {
                    "operation": "insert",
                    "unid": data['unid'],  # 使用unid作为主键标识
                    "primary_keys": {"unid": data['unid']},
                    "record_id": record_id,
                    "data": data
                }
                inserted_records.append(record_info)

            process_logger.log_end(
                "保存项目资金数据到数据库",
                插入记录数=len(inserted_records),
                更新记录数=0  # 经费数据永远不更新
            )

            return inserted_records, updated_records

        except Exception as e:
            process_logger.log_error("保存项目资金数据", str(e))
            raise

    def save_transactions_data(self, df: pd.DataFrame) -> Tuple[List[Dict], List[Dict]]:
        """保存收支明细数据到数据库
        
        Args:
            df: 收支明细数据DataFrame
            
        Returns:
            Tuple[List[Dict], List[Dict]]: (插入的记录, 更新的记录)
        """
        process_logger.log_start("保存收支明细数据到数据库")

        inserted_records = []
        updated_records = []

        try:
            # 批量查询所有fundid的opening_balance状态（首次保存逻辑）
            fundids = df['fundid'].unique().tolist()
            fundid_has_opening_balance = {}  # {fundid: bool} 记录哪些fundid已有期初余额

            if fundids:
                # 构建IN查询，查询哪些fundid已有非NULL的openingbalance
                placeholders = ','.join(['?' for _ in fundids])
                query = f"""
                    SELECT DISTINCT fundid, openingbalance
                    FROM transactions
                    WHERE fundid IN ({placeholders})
                    AND openingbalance IS NOT NULL
                """
                cursor = self.connection.cursor()
                cursor.execute(query, fundids)
                results = cursor.fetchall()

                # 标记哪些fundid已有期初余额
                for row in results:
                    fundid = row[0]
                    fundid_has_opening_balance[fundid] = True

                process_logger.logger.info(f"批量查询期初余额状态: {len(fundid_has_opening_balance)}个经费卡号已有期初余额")

            for index, row in df.iterrows():
                # 准备数据（排除空值和NaN）
                data = {}
                for col, value in row.items():
                    if pd.notna(value) and value != "":
                        # 处理datetime类型
                        if hasattr(value, 'strftime'):
                            data[col] = value.strftime('%Y-%m-%d %H:%M:%S')
                        else:
                            data[col] = value
                
                # 联合主键：fundid, transactiondate, vouchernumber, debitamount, balance
                primary_keys = {
                    "fundid": data.get("fundid"),
                    "transactiondate": data.get("transactiondate"),
                    "vouchernumber": data.get("vouchernumber"),
                    "debitamount": data.get("debitamount"),
                    "balance": data.get("balance")
                }

                # 检查记录是否存在
                existing_record = self.check_record_exists("transactions", primary_keys)

                if existing_record:
                    # 更新记录
                    update_data = data.copy()

                    # 首次保存逻辑：如果该fundid已有期初余额，则不更新openingbalance字段
                    current_fundid = data.get("fundid")
                    if current_fundid and fundid_has_opening_balance.get(current_fundid, False):
                        update_data.pop("openingbalance", None)
                        process_logger.logger.debug(f"经费卡号{current_fundid}已有期初余额，不更新openingbalance字段")

                    # 移除主键字段，避免在UPDATE语句中更新主键
                    for key in primary_keys.keys():
                        update_data.pop(key, None)
                    update_data['updated_at'] = 'CURRENT_TIMESTAMP'
                    
                    if update_data:  # 确保有数据需要更新
                        self.update_record("transactions", update_data, primary_keys)
                        
                        # 记录更新信息（包含updateid用于OA同步）
                        record_info = {
                            "operation": "update",
                            "primary_keys": primary_keys,
                            "updateid": existing_record.get("updateid"),
                            "data": data
                        }
                        updated_records.append(record_info)
                else:
                    # 插入新记录
                    insert_data = data.copy()

                    # 首次保存逻辑：如果该fundid已有期初余额，则不插入openingbalance字段
                    current_fundid = data.get("fundid")
                    if current_fundid and fundid_has_opening_balance.get(current_fundid, False):
                        insert_data.pop("openingbalance", None)
                        process_logger.logger.debug(f"经费卡号{current_fundid}已有期初余额，不插入openingbalance字段")

                    insert_data['created_at'] = 'CURRENT_TIMESTAMP'
                    insert_data['updated_at'] = 'CURRENT_TIMESTAMP'
                    record_id = self.insert_record("transactions", insert_data)
                    
                    # 记录插入信息
                    record_info = {
                        "operation": "insert",
                        "primary_keys": primary_keys,
                        "record_id": record_id,
                        "data": data
                    }
                    inserted_records.append(record_info)
            
            process_logger.log_end(
                "保存收支明细数据到数据库",
                插入记录数=len(inserted_records),
                更新记录数=len(updated_records)
            )
            
            return inserted_records, updated_records
            
        except Exception as e:
            process_logger.log_error("保存收支明细数据", str(e))
            raise
    
    def update_oa_sync_id(self, table_name: str, primary_keys: Dict[str, Any], oa_id: str):
        """更新OA同步ID
        
        Args:
            table_name: 表名
            primary_keys: 主键字段和值
            oa_id: OA系统ID
        """
        try:
            self.update_record(table_name, {"updateid": oa_id}, primary_keys)
            process_logger.log_oa_operation("更新OA同步ID", table_name=table_name, oa_id=oa_id)
        except Exception as e:
            process_logger.log_error("更新OA同步ID", str(e))
            raise
    
    def __enter__(self):
        """上下文管理器入口"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器出口"""
        self.close_connection() 