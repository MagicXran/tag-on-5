#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据库重建脚本 - UUID集成版本

此脚本用于重建数据库表结构以支持UUID功能
警告：此操作会清空所有现有数据！
"""

import sys
import os

# 添加项目根目录到路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from database_manager_sqlite import DatabaseManager
from logger_utils import process_logger
import time


def confirm_rebuild():
    """确认重建操作"""
    print("⚠️  警告：此操作将重建数据库表结构并清空所有现有数据！")
    print("   - 所有合同、经费、收支明细数据将被删除")
    print("   - 表结构将被重新创建以支持UUID功能") 
    print("   - 此操作不可逆转！")
    print()
    
    while True:
        confirm = input("是否确认继续？请输入 'YES' 确认，其他任意键取消: ").strip()
        if confirm == "YES":
            return True
        elif confirm.lower() in ["no", "n", "cancel", "取消"]:
            print("操作已取消")
            return False
        else:
            print("请输入 'YES' 确认操作，或输入其他内容取消")


def backup_recommendation():
    """备份建议"""
    print("💡 建议在继续之前：")
    print("   1. 备份重要的Excel原始数据文件")
    print("   2. 如有必要，导出现有数据库数据")
    print("   3. 确认可以重新导入所需的数据")
    print()
    
    proceed = input("是否已完成备份准备？(y/N): ").strip().lower()
    return proceed in ["y", "yes", "是"]


def rebuild_database():
    """重建数据库"""
    print("\n开始重建数据库...")
    
    try:
        # 创建数据库管理器
        db_manager = DatabaseManager()
        
        # 显示当前表状态
        print("检查当前表状态...")
        validation_before = db_manager.validate_table_structure()
        print(f"重建前表结构验证: {validation_before}")
        
        # 执行重建
        print("正在重建数据库表...")
        success = db_manager.rebuild_database_tables(confirm_rebuild=True)
        
        if success:
            print("✅ 数据库表重建成功")
            
            # 验证重建结果
            print("验证重建结果...")
            validation_after = db_manager.validate_table_structure()
            print(f"重建后表结构验证: {validation_after}")
            
            # 检查所有表都有UUID字段
            expected_tables = ["contracts", "projectfunds", "transactions"]
            all_valid = all(validation_after.get(table) == True for table in expected_tables)
            
            if all_valid:
                print("✅ 所有表已成功添加UUID字段")
                
                # 优化查询性能
                print("优化UUID查询性能...")
                optimization_results = db_manager.optimize_uuid_queries()
                print(f"优化结果: {optimization_results}")
                
                # 获取性能统计
                stats = db_manager.get_uuid_performance_stats()
                print(f"性能统计: {stats}")
                
                return True
            else:
                print("❌ 部分表缺少UUID字段，请检查")
                return False
        else:
            print("❌ 数据库表重建失败")
            return False
            
    except Exception as e:
        print(f"❌ 重建过程中发生错误: {e}")
        process_logger.log_error("数据库重建", str(e))
        return False


def post_rebuild_instructions():
    """重建后说明"""
    print("\n🎉 数据库重建完成！")
    print("\n接下来的步骤：")
    print("1. 运行测试脚本验证UUID功能：")
    print("   python test_uuid_integration.py")
    print()
    print("2. 重新导入数据：")
    print("   - 合同数据：使用GUI或命令行工具导入Excel文件")
    print("   - 经费数据：重新处理经费到账清单")
    print("   - 收支明细：重新处理收支明细表")
    print()
    print("3. 验证功能：")
    print("   - 检查数据导入时是否自动生成UUID")
    print("   - 测试OA同步功能是否包含UUID信息")
    print("   - 验证查询和性能是否正常")
    print()
    print("4. 查看UUID集成指南：")
    print("   cat UUID_INTEGRATION_GUIDE.md")


def main():
    """主函数"""
    print("RMS系统数据库重建工具 - UUID集成版本")
    print("=" * 50)
    
    try:
        # 显示备份建议
        if not backup_recommendation():
            print("建议完成备份后再重新运行此脚本")
            return False
        
        # 确认重建操作
        if not confirm_rebuild():
            return False
        
        # 最后确认
        print("\n⏰ 等待5秒钟，按Ctrl+C可以取消...")
        try:
            for i in range(5, 0, -1):
                print(f"   {i}秒后开始重建...")
                time.sleep(1)
        except KeyboardInterrupt:
            print("\n操作已被用户取消")
            return False
        
        # 执行重建
        success = rebuild_database()
        
        if success:
            post_rebuild_instructions()
            return True
        else:
            print("\n❌ 重建失败，请检查错误信息并重试")
            return False
            
    except KeyboardInterrupt:
        print("\n操作被用户中断")
        return False
    except Exception as e:
        print(f"\n❌ 重建过程发生异常: {e}")
        return False


if __name__ == "__main__":
    success = main()
    print("\n" + "=" * 50)
    if success:
        print("重建完成！")
    else:
        print("重建失败或被取消")
    sys.exit(0 if success else 1)