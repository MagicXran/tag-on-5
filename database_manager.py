"""
数据库管理模块 - 负责MySQL数据库的连接、读取、插入和更新操作
"""

import pymysql
import pandas as pd
from typing import Dict, List, Tuple, Optional, Any, Union
from config import MYSQL_CONFIG
from logger_utils import process_logger


class DatabaseManager:
    """数据库管理器，负责MySQL数据库操作"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or MYSQL_CONFIG
        self.connection = None

    
    def validate_config(self) -> Tuple[bool, str]:
        """验证数据库配置
        
        Returns:
            Tuple[bool, str]: (配置是否有效, 详细信息)
        """
        required_fields = ['host', 'user', 'password', 'database']
        missing_fields = []
        
        for field in required_fields:
            if field not in self.config or not self.config[field]:
                missing_fields.append(field)
        
        if missing_fields:
            return False, f"缺少必需的配置项: {', '.join(missing_fields)}"
        
        # 检查端口配置
        port = self.config.get('port', 3306)
        if not isinstance(port, int) or port <= 0 or port > 65535:
            return False, f"端口配置无效: {port}"
        
        return True, "配置验证通过"

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
            "pymysql_version": "",
            "detailed_error": ""
        }
        
        try:
            # 获取PyMySQL版本
            diagnostic_info["pymysql_version"] = pymysql.__version__
            
            # 验证配置
            config_valid, config_message = self.validate_config()
            diagnostic_info["config_valid"] = config_valid
            diagnostic_info["config_message"] = config_message
            
            if not config_valid:
                return diagnostic_info
            
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
        if self.connection is None or not self.connection.open:
            try:
                # 记录连接尝试信息
                process_logger.logger.info(f"数据库连接 - 尝试连接到 {self.config['host']}:{self.config.get('port', 3306)}")
                
                self.connection = pymysql.connect(
                    host=self.config['host'],
                    user=self.config['user'],
                    password=self.config['password'],
                    database=self.config['database'],
                    charset=self.config.get('charset', 'utf8mb4'),
                    port=self.config.get('port', 3306),
                    cursorclass=pymysql.cursors.DictCursor,
                    autocommit=False,
                    connect_timeout=30  # 30秒连接超时
                )
                
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
            # 首先测试基本连接
            conn = self.get_connection()
            if not conn:
                return False, "无法建立数据库连接"
            
            # 测试连接是否活跃
            if not conn.open:
                return False, "数据库连接已关闭"
            
            # 执行简单查询测试
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1 as test_value")
                result = cursor.fetchone()
                if result and result.get('test_value') == 1:
                    return True, "数据库连接成功"
                else:
                    return False, "数据库查询返回异常结果"
                    
        except pymysql.err.OperationalError as e:
            error_msg = f"数据库操作错误: {str(e)}"
            process_logger.log_error("数据库连接测试", error_msg)
            return False, error_msg
        except pymysql.err.ProgrammingError as e:
            error_msg = f"数据库编程错误: {str(e)}"
            process_logger.log_error("数据库连接测试", error_msg)
            return False, error_msg
        except Exception as e:
            error_msg = f"未知错误: {str(e)}"
            process_logger.log_error("数据库连接测试", error_msg)
            return False, error_msg
    
    def close_connection(self):
        """关闭数据库连接"""
        if self.connection and self.connection.open:
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
            with conn.cursor() as cursor:
                cursor.execute(sql, params)
                if fetch_one:
                    return cursor.fetchone()
                elif fetch_all:
                    return cursor.fetchall()
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
            with conn.cursor() as cursor:
                affected_rows = cursor.execute(sql, params)
                conn.commit()
                return affected_rows
        except Exception as e:
            conn.rollback()
            process_logger.log_error("数据库更新", str(e), sql=sql)
            raise
    
    def execute_batch_update(self, sql: str, params_list: List[tuple]):
        """批量执行更新语句
        
        Args:
            sql: SQL语句
            params_list: 参数列表
            
        Returns:
            总影响的行数
        """
        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                total_affected = 0
                for params in params_list:
                    affected_rows = cursor.execute(sql, params)
                    total_affected += affected_rows
                conn.commit()
                return total_affected
        except Exception as e:
            conn.rollback()
            process_logger.log_error("数据库批量更新", str(e), sql=sql)
            raise
    
    def check_record_exists(self, table_name: str, primary_keys: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """检查记录是否存在
        
        Args:
            table_name: 表名
            primary_keys: 主键字段和值的字典
            
        Returns:
            存在的记录或None
        """
        where_conditions = []
        where_params = []
        
        for key, value in primary_keys.items():
            where_conditions.append(f"{key} = %s")
            where_params.append(value)
        
        where_clause = " AND ".join(where_conditions)
        sql = f"SELECT * FROM {table_name} WHERE {where_clause}"
        
        result = self.execute_query(sql, tuple(where_params), fetch_one=True, fetch_all=False)
        return result
    
    def insert_record(self, table_name: str, data: Dict[str, Any]) -> int:
        """插入单条记录
        
        Args:
            table_name: 表名
            data: 数据字典
            
        Returns:
            新插入记录的ID
        """
        columns = list(data.keys())
        values = list(data.values())
        placeholders = ", ".join(["%s"] * len(values))
        
        sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
        
        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(sql, tuple(values))
                record_id = cursor.lastrowid
                conn.commit()
                
                process_logger.log_database_operation(
                    "插入",
                    table_name,
                    affected_rows=1,
                    record_id=record_id
                )
                
                return record_id
        except Exception as e:
            conn.rollback()
            process_logger.log_error("数据库插入", str(e), table=table_name, data=data)
            raise
    
    def update_record(self, table_name: str, data: Dict[str, Any], primary_keys: Dict[str, Any]) -> int:
        """更新单条记录
        
        Args:
            table_name: 表名
            data: 更新的数据字典
            primary_keys: 主键字段和值的字典
            
        Returns:
            影响的行数
        """
        set_conditions = []
        set_params = []
        
        for key, value in data.items():
            set_conditions.append(f"{key} = %s")
            set_params.append(value)
        
        where_conditions = []
        where_params = []
        
        for key, value in primary_keys.items():
            where_conditions.append(f"{key} = %s")
            where_params.append(value)
        
        set_clause = ", ".join(set_conditions)
        where_clause = " AND ".join(where_conditions)
        
        sql = f"UPDATE {table_name} SET {set_clause} WHERE {where_clause}"
        params = tuple(set_params + where_params)
        
        affected_rows = self.execute_update(sql, params)
        
        process_logger.log_database_operation(
            "更新",
            table_name,
            affected_rows=affected_rows,
            primary_keys=primary_keys
        )
        
        return affected_rows
    
    def upsert_records(self, table_name: str, records: List[Dict[str, Any]], key_fields: List[str]) -> Dict[str, Any]:
        """批量插入或更新记录
        
        Args:
            table_name: 表名
            records: 记录列表
            key_fields: 用于判断记录是否存在的关键字段
            
        Returns:
            处理结果统计
        """
        result = {"inserted": 0, "updated": 0, "errors": []}
        
        if not records:
            return result
        
        for record in records:
            try:
                # 构建主键条件
                primary_keys = {}
                for key_field in key_fields:
                    if key_field in record:
                        primary_keys[key_field] = record[key_field]
                
                if not primary_keys:
                    # 如果没有主键信息，直接插入
                    self.insert_record(table_name, record)
                    result["inserted"] += 1
                    continue
                
                # 检查记录是否存在
                existing_record = self.check_record_exists(table_name, primary_keys)
                
                if existing_record:
                    # 更新现有记录
                    update_data = record.copy()
                    # 移除主键字段，避免在UPDATE语句中更新主键
                    for key_field in key_fields:
                        update_data.pop(key_field, None)
                    
                    if update_data:  # 确保有数据需要更新
                        self.update_record(table_name, update_data, primary_keys)
                        result["updated"] += 1
                else:
                    # 插入新记录
                    self.insert_record(table_name, record)
                    result["inserted"] += 1
                    
            except Exception as e:
                error_msg = f"处理记录失败: {str(e)}"
                result["errors"].append(error_msg)
                process_logger.log_error("记录upsert", error_msg, record=record)
        
        process_logger.log_database_operation(
            "批量upsert",
            table_name,
            inserted=result["inserted"],
            updated=result["updated"],
            errors=len(result["errors"])
        )
        
        return result
    
    def save_contracts_data(self, df: pd.DataFrame) -> Tuple[List[Dict], List[Dict]]:
        """保存合同数据到数据库
        
        Args:
            df: 清洗后的合同数据DataFrame
            
        Returns:
            Tuple[List[Dict], List[Dict]]: (插入的记录列表, 更新的记录列表)
        """
        process_logger.log_start("保存合同数据", 记录数=len(df))
        
        inserted_records = []
        updated_records = []
        
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
            
            # 主键是contractid
            primary_key = {"contractid": data.get("contractid")}
            
            # 检查记录是否存在
            existing_record = self.check_record_exists("contracts", primary_key)
            
            if existing_record:
                # 更新记录
                update_data = data.copy()
                # 移除主键字段，避免在UPDATE语句中更新主键
                update_data.pop("contractid", None)
                
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
                record_id = self.insert_record("contracts", data)
                
                # 记录插入信息
                record_info = {
                    "operation": "insert",
                    "contractid": data.get("contractid"),
                    "record_id": record_id,
                    "data": data
                }
                inserted_records.append(record_info)
        
        process_logger.log_end(
            "保存合同数据",
            插入记录数=len(inserted_records),
            更新记录数=len(updated_records)
        )
        
        return inserted_records, updated_records
    
    def save_project_funds_data(self, df: pd.DataFrame) -> Tuple[List[Dict], List[Dict]]:
        """保存经费到账数据到数据库
        
        Args:
            df: 清洗后的经费到账数据DataFrame
            
        Returns:
            Tuple[List[Dict], List[Dict]]: (插入的记录列表, 更新的记录列表)
        """
        process_logger.log_start("保存经费到账数据", 记录数=len(df))
        
        inserted_records = []
        updated_records = []
        
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
            
            # 使用配置中的联合主键：fundid, allocation_date, contractid
            from config import DATABASE_PRIMARY_KEYS
            primary_key_fields = DATABASE_PRIMARY_KEYS["projectfunds"]
            primary_keys = {key: data.get(key) for key in primary_key_fields}
            
            # 检查记录是否存在
            existing_record = self.check_record_exists("projectfunds", primary_keys)
            
            if existing_record:
                # 更新记录
                update_data = data.copy()
                # 移除主键字段，避免在UPDATE语句中更新主键
                for key in primary_keys.keys():
                    update_data.pop(key, None)
                
                if update_data:  # 确保有数据需要更新
                    self.update_record("projectfunds", update_data, primary_keys)
                    
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
                record_id = self.insert_record("projectfunds", data)
                
                # 记录插入信息
                record_info = {
                    "operation": "insert",
                    "primary_keys": primary_keys,
                    "record_id": record_id,
                    "data": data
                }
                inserted_records.append(record_info)
        
        process_logger.log_end(
            "保存经费到账数据",
            插入记录数=len(inserted_records),
            更新记录数=len(updated_records)
        )
        
        return inserted_records, updated_records
    
    def update_oa_sync_id(self, table_name: str, primary_keys: Dict[str, Any], oa_id: str):
        """更新OA同步ID
        
        Args:
            table_name: 表名
            primary_keys: 主键字段和值的字典
            oa_id: OA系统返回的ID
        """
        update_data = {"updateid": oa_id}
        self.update_record(table_name, update_data, primary_keys)
        
        process_logger.log_database_operation(
            "更新OA同步ID",
            table_name,
            affected_rows=1,
            oa_id=oa_id,
            primary_keys=primary_keys
        )
    
    def __enter__(self):
        """上下文管理器入口"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器退出"""
        self.close_connection() 