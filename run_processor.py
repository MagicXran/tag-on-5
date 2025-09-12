#!/usr/bin/env python3
"""
RMS标签导入处理系统主入口文件
运行此文件启动完整的数据处理流程
"""

import os
import sys
import asyncio
from datetime import datetime

# 添加当前目录到Python路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from main_processor import RMSTagOnProcessor, ProcessSummaryReporter
from logger_utils import process_logger


def main():
    """主函数 - 启动数据处理流程"""
    print("=" * 60)
    print("RMS标签导入处理系统启动")
    print("=" * 60)
    
    start_time = datetime.now()
    
    try:
        # 创建处理器实例
        processor = RMSTagOnProcessor()
        
        # 执行完整的数据处理流程
        print("正在启动数据处理流程...")
        results = processor.run_sync_process()
        
        # 生成并显示处理结果摘要
        summary_report = ProcessSummaryReporter.generate_summary_report(results)
        print(summary_report)
        
        # 记录处理完成时间
        end_time = datetime.now()
        duration = end_time - start_time
        
        print(f"处理总耗时: {duration.total_seconds():.2f} 秒")
        
        # 判断处理结果
        if not results.get("errors"):
            print("✅ 数据处理成功完成！")
            return 0
        else:
            print("⚠️  数据处理完成，但存在错误！")
            return 1
    
    except KeyboardInterrupt:
        print("\n❌ 用户中断处理流程")
        process_logger.log_warning("用户操作", "用户中断了处理流程")
        return 2
    
    except Exception as e:
        print(f"\n❌ 处理流程发生未预期错误: {str(e)}")
        process_logger.log_error("主程序", str(e))
        return 3


def check_environment():
    """检查运行环境"""
    print("检查运行环境...")
    
    # 检查Python版本
    if sys.version_info < (3, 7):
        print("❌ 需要Python 3.7或更高版本")
        return False
    
    # 检查必要的模块
    required_modules = ['pandas', 'pymysql', 'aiohttp', 'xlrd']
    missing_modules = []
    
    for module in required_modules:
        try:
            __import__(module)
        except ImportError:
            missing_modules.append(module)
    
    if missing_modules:
        print(f"❌ 缺少必要的Python模块: {', '.join(missing_modules)}")
        print("请使用以下命令安装:")
        print(f"pip install {' '.join(missing_modules)}")
        return False
    
    print("✅ 运行环境检查通过")
    return True


def display_help():
    """显示帮助信息"""
    help_text = """
RMS标签导入处理系统 - 使用说明

功能描述:
  本系统从Excel文件读取合同签订清单和经费到账清单数据，
  进行数据清洗、筛选后保存到MySQL数据库，
  并通过OA接口同步数据到OA系统。

运行方式:
  python run_processor.py [选项]

选项:
  --help, -h      显示此帮助信息
  --check, -c     仅检查运行环境
  --version, -v   显示版本信息

配置文件:
  config.py       包含所有系统配置信息

日志文件:
  logs/           日志文件存储目录

注意事项:
  1. 确保Excel文件路径在config.py中正确配置
  2. 确保MySQL数据库连接信息正确
  3. 确保OA系统配置信息正确
  4. 运行前请备份重要数据
"""
    print(help_text)


if __name__ == "__main__":
    # 处理命令行参数
    if len(sys.argv) > 1:
        arg = sys.argv[1].lower()
        
        if arg in ('--help', '-h'):
            display_help()
            sys.exit(0)
        elif arg in ('--check', '-c'):
            if check_environment():
                print("✅ 环境检查完成，系统可以正常运行")
                sys.exit(0)
            else:
                print("❌ 环境检查失败，请解决问题后重试")
                sys.exit(1)
        elif arg in ('--version', '-v'):
            print("RMS标签导入处理系统 v1.0.0")
            sys.exit(0)
        else:
            print(f"未知参数: {arg}")
            print("使用 --help 查看帮助信息")
            sys.exit(1)
    
    # 检查环境
    if not check_environment():
        sys.exit(1)
    
    # 运行主程序
    exit_code = main()
    sys.exit(exit_code) 