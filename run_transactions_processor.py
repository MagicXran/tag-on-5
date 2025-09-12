#!/usr/bin/env python3
"""
收支明细表处理器运行脚本
提供命令行界面用于处理收支明细表Excel文件
"""

import os
import sys
import argparse
import traceback
from pathlib import Path
from datetime import datetime

# 添加项目根目录到路径
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from transactions_processor import TransactionsProcessor
from config import EXCEL_FILES, RUNTIME_CONFIG
from logger_utils import LoggerManager


def parse_arguments():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(
        description="收支明细表处理器 - 批量处理Excel收支明细表文件",
        epilog="示例用法:\n"
               "  python run_transactions_processor.py\n"
               "  python run_transactions_processor.py --folder /path/to/folder\n"
               "  python run_transactions_processor.py --verbose\n"
               "  python run_transactions_processor.py --no-oa-sync",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument(
        "--folder", "-f",
        type=str,
        help="指定收支明细表文件夹路径（如不指定则使用配置文件中的路径）"
    )
    
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="启用详细输出模式，显示更多调试信息"
    )
    
    parser.add_argument(
        "--no-oa-sync",
        action="store_true",
        help="禁用OA系统同步，仅处理到数据库"
    )
    
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="预运行模式，仅解析文件不保存到数据库"
    )
    
    parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="批处理大小（默认：100）"
    )
    
    return parser.parse_args()


def setup_logger(verbose: bool = False):
    """设置日志记录器"""
    log_level = "DEBUG" if verbose else "INFO"
    logger = LoggerManager.get_logger("run_transactions_processor", level=log_level)
    return logger


def validate_folder_path(folder_path: str, logger) -> bool:
    """验证文件夹路径是否有效"""
    if not folder_path:
        logger.error("未指定文件夹路径")
        return False
    
    if not os.path.exists(folder_path):
        logger.error(f"指定的文件夹不存在: {folder_path}")
        return False
    
    if not os.path.isdir(folder_path):
        logger.error(f"指定的路径不是文件夹: {folder_path}")
        return False
    
    # 检查文件夹是否包含Excel文件
    excel_extensions = ['.xlsx', '.xls']
    excel_files = []
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            if any(file.lower().endswith(ext) for ext in excel_extensions):
                excel_files.append(file)
    
    if not excel_files:
        logger.warning(f"文件夹中未找到Excel文件: {folder_path}")
        return False
    
    logger.info(f"在文件夹中找到 {len(excel_files)} 个Excel文件")
    return True


def print_processing_summary(results: dict, logger):
    """打印处理结果摘要"""
    print("\n" + "="*60)
    print("收支明细表处理结果摘要")
    print("="*60)
    
    print(f"处理文件数: {results.get('processed_files', 0)}")
    print(f"总记录数: {results.get('total_records', 0)}")
    print(f"成功记录数: {results.get('successful_records', 0)}")
    print(f"失败记录数: {results.get('failed_records', 0)}")
    
    # 计算成功率
    total_records = results.get('total_records', 0)
    if total_records > 0:
        success_rate = (results.get('successful_records', 0) / total_records) * 100
        print(f"成功率: {success_rate:.2f}%")
    
    # 显示错误信息
    errors = results.get('errors', [])
    if errors:
        print(f"\n错误信息 ({len(errors)}条):")
        for i, error in enumerate(errors[:10], 1):  # 只显示前10条错误
            print(f"  {i}. {error}")
        if len(errors) > 10:
            print(f"  ... 还有 {len(errors) - 10} 条错误")
    
    print("="*60)


def show_configuration_info(logger):
    """显示配置信息"""
    logger.info("系统配置信息:")
    logger.info(f"  - 收支明细表文件夹: {EXCEL_FILES.get('transactions', '未配置')}")
    logger.info(f"  - OA同步状态: {'启用' if RUNTIME_CONFIG.get('enable_oa_sync', False) else '禁用'}")
    logger.info(f"  - 主从表模式: {'启用' if RUNTIME_CONFIG.get('enable_oa_master_sub_table', False) else '禁用'}")
    logger.info(f"  - 批处理大小: {RUNTIME_CONFIG.get('batch_size', 100)}")


def main():
    """主程序入口"""
    try:
        # 解析命令行参数
        args = parse_arguments()
        
        # 设置日志记录器
        logger = setup_logger(args.verbose)
        
        # 显示程序信息
        logger.info("收支明细表处理器启动")
        logger.info(f"运行时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # 显示配置信息
        if args.verbose:
            show_configuration_info(logger)
        
        # 确定文件夹路径
        folder_path = args.folder or EXCEL_FILES.get("transactions")
        if not folder_path:
            logger.error("未指定文件夹路径，请使用 --folder 参数或在配置文件中设置")
            sys.exit(1)
        
        logger.info(f"使用文件夹路径: {folder_path}")
        
        # 验证文件夹路径
        if not validate_folder_path(folder_path, logger):
            logger.error("文件夹路径验证失败")
            sys.exit(1)
        
        # 创建处理器实例
        logger.info("初始化收支明细表处理器...")
        processor = TransactionsProcessor()
        
        # 应用命令行参数
        if args.no_oa_sync:
            logger.info("禁用OA同步")
            processor.oa_manager = None
        
        if args.batch_size != 100:
            logger.info(f"设置批处理大小: {args.batch_size}")
            # 这里可以根据需要调整批处理大小
        
        # 预运行模式检查
        if args.dry_run:
            logger.info("预运行模式：仅解析文件，不保存到数据库")
            # 这里可以实现预运行逻辑
            print("预运行模式尚未实现")
            return
        
        # 开始处理
        logger.info("开始处理收支明细表文件...")
        print("正在处理文件，请稍候...")
        
        results = processor.process_transactions_folder(folder_path)
        
        # 显示处理结果
        print_processing_summary(results, logger)
        
        # 判断处理是否成功
        if results.get('processed_files', 0) > 0:
            logger.info("收支明细表处理完成")
            if results.get('errors'):
                logger.warning("处理过程中出现了一些错误，请查看日志了解详情")
                sys.exit(2)  # 部分成功
            else:
                print("\n✓ 所有文件处理成功！")
                sys.exit(0)  # 完全成功
        else:
            logger.error("没有成功处理任何文件")
            sys.exit(1)  # 失败
    
    except KeyboardInterrupt:
        print("\n\n用户中断操作")
        sys.exit(130)
    
    except Exception as e:
        logger = LoggerManager.get_logger("run_transactions_processor")
        logger.error(f"程序执行过程中发生未预期的错误: {str(e)}")
        logger.error(f"错误详情: {traceback.format_exc()}")
        
        print(f"\n❌ 程序执行失败: {str(e)}")
        if args.verbose:
            print(f"错误堆栈:\n{traceback.format_exc()}")
        
        sys.exit(1)


def show_help():
    """显示帮助信息"""
    print("""
收支明细表处理器使用说明
========================

基本用法:
  python run_transactions_processor.py

常用选项:
  --folder, -f PATH     指定收支明细表文件夹路径
  --verbose, -v         启用详细输出模式
  --no-oa-sync         禁用OA系统同步
  --dry-run            预运行模式（仅解析不保存）
  --batch-size SIZE    设置批处理大小
  --help, -h           显示帮助信息

示例:
  # 使用配置文件中的路径处理
  python run_transactions_processor.py
  
  # 指定文件夹路径
  python run_transactions_processor.py --folder /path/to/transactions
  
  # 详细输出模式
  python run_transactions_processor.py --verbose
  
  # 禁用OA同步
  python run_transactions_processor.py --no-oa-sync

配置文件:
  请确保在 config.py 中正确配置以下信息：
  - EXCEL_FILES["transactions"]: 收支明细表文件夹路径
  - MYSQL_CONFIG: 数据库连接配置
  - OA_CONFIG: OA系统配置（如需同步）

支持的文件格式:
  - Excel文件 (.xlsx, .xls)
  - 符合指定格式的收支明细表结构

注意事项:
  1. 确保文件夹路径存在且可访问
  2. 确保数据库连接正常
  3. 如需OA同步，请确保OA系统配置正确
  4. 处理大量文件时建议使用 --verbose 查看详细进度

更多信息请参考: README_TRANSACTIONS.md
""")


if __name__ == "__main__":
    main() 