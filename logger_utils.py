"""
日志工具模块 - 提供结构化日志记录功能
"""

import logging
import os
from datetime import datetime
from logging.handlers import TimedRotatingFileHandler
from config import LOG_CONFIG


class LoggerManager:
    """日志管理器，负责创建和配置日志记录器"""
    
    _loggers = {}
    
    @classmethod
    def get_logger(cls, name="rms_tag_on", level=None):
        """获取日志记录器实例
        
        Args:
            name: 日志记录器名称
            level: 日志级别
            
        Returns:
            logging.Logger: 配置好的日志记录器
        """
        if name in cls._loggers:
            return cls._loggers[name]
        
        logger = cls._create_logger(name, level)
        cls._loggers[name] = logger
        return logger
    
    @classmethod
    def _create_logger(cls, name, level=None):
        """创建新的日志记录器
        
        Args:
            name: 日志记录器名称  
            level: 日志级别
            
        Returns:
            logging.Logger: 配置好的日志记录器
        """
        logger = logging.getLogger(name)
        
        if logger.hasHandlers():
            return logger
            
        # 设置日志级别
        log_level = level or getattr(logging, LOG_CONFIG.get("log_level", "INFO"))
        logger.setLevel(log_level)
        
        # 创建日志目录
        log_dir = LOG_CONFIG.get("log_dir")
        if not os.path.exists(log_dir):
            os.makedirs(log_dir, exist_ok=True)
        
        # 生成日志文件路径
        today = datetime.now().strftime("%Y%m%d")
        log_filename = LOG_CONFIG.get("log_file_format", "rms_tag_on_{date}.log").format(date=today)
        log_filepath = os.path.join(log_dir, log_filename)
        
        # 创建文件处理器
        file_handler = logging.FileHandler(log_filepath, encoding='utf-8')
        file_handler.setLevel(log_level)
        
        # 创建控制台处理器
        console_handler = logging.StreamHandler()
        console_handler.setLevel(log_level)
        
        # 创建格式化器
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        
        # 添加处理器到日志记录器
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
        
        return logger


class ProcessLogger:
    """业务处理日志记录器，提供结构化的业务日志记录方法"""
    
    def __init__(self, logger_name="rms_tag_on"):
        self.logger = LoggerManager.get_logger(logger_name)
    
    def log_start(self, process_name, **kwargs):
        """记录处理开始日志
        
        Args:
            process_name: 处理过程名称
            **kwargs: 额外的上下文信息
        """
        context_info = " | ".join([f"{k}={v}" for k, v in kwargs.items()])
        self.logger.info(f"开始执行: {process_name} | {context_info}")
    
    def log_end(self, process_name, **kwargs):
        """记录处理结束日志
        
        Args:
            process_name: 处理过程名称
            **kwargs: 额外的上下文信息
        """
        context_info = " | ".join([f"{k}={v}" for k, v in kwargs.items()])
        self.logger.info(f"完成执行: {process_name} | {context_info}")
    
    def log_data_stats(self, operation, table_name, **stats):
        """记录数据统计信息
        
        Args:
            operation: 操作类型 (读取/清洗/保存等)
            table_name: 表名
            **stats: 统计数据
        """
        stats_info = " | ".join([f"{k}={v}" for k, v in stats.items()])
        self.logger.info(f"数据统计 - {operation} - {table_name}: {stats_info}")
    
    def log_filter_result(self, filter_name, original_count, filtered_count, filter_conditions=None):
        """记录筛选结果
        
        Args:
            filter_name: 筛选器名称
            original_count: 原始记录数
            filtered_count: 筛选后记录数
            filter_conditions: 筛选条件描述
        """
        conditions_desc = f" | 条件: {filter_conditions}" if filter_conditions else ""
        self.logger.info(
            f"筛选结果 - {filter_name}: {original_count} -> {filtered_count} 条记录{conditions_desc}"
        )
    
    def log_database_operation(self, operation, table_name, affected_rows=None, **details):
        """记录数据库操作
        
        Args:
            operation: 操作类型 (插入/更新/删除等)
            table_name: 表名
            affected_rows: 影响的行数
            **details: 操作详情
        """
        details_info = " | ".join([f"{k}={v}" for k, v in details.items()])
        affected_info = f" | 影响行数: {affected_rows}" if affected_rows is not None else ""
        self.logger.info(f"数据库操作 - {operation} - {table_name}{affected_info} | {details_info}")
    
    def log_oa_operation(self, operation, **details):
        """记录OA系统操作
        
        Args:
            operation: 操作类型
            **details: 操作详情
        """
        details_info = " | ".join([f"{k}={v}" for k, v in details.items()])
        self.logger.info(f"OA操作 - {operation}: {details_info}")
    
    def log_error(self, error_type, error_msg, **context):
        """记录错误信息
        
        Args:
            error_type: 错误类型
            error_msg: 错误消息
            **context: 错误上下文
        """
        context_info = " | ".join([f"{k}={v}" for k, v in context.items()]) if context else ""
        self.logger.error(f"错误 - {error_type}: {error_msg} | {context_info}")
    
    def log_warning(self, warning_type, warning_msg, **context):
        """记录警告信息
        
        Args:
            warning_type: 警告类型
            warning_msg: 警告消息
            **context: 警告上下文
        """
        context_info = " | ".join([f"{k}={v}" for k, v in context.items()]) if context else ""
        self.logger.warning(f"警告 - {warning_type}: {warning_msg} | {context_info}")
    
    def log_excel_operation(self, operation, file_path, **details):
        """记录Excel文件操作
        
        Args:
            operation: 操作类型
            file_path: 文件路径
            **details: 操作详情
        """
        details_info = " | ".join([f"{k}={v}" for k, v in details.items()])
        self.logger.info(f"Excel操作 - {operation} - {file_path}: {details_info}")


# 全局日志实例
process_logger = ProcessLogger() 