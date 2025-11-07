"""
测试收支明细表唯一约束修复
验证5字段唯一约束是否正常工作
"""

import os
import sys
from datetime import datetime
from database_manager_sqlite import DatabaseManager
from config import SQLITE_CONFIG, TRANSACTIONS_PRIMARY_KEYS
import pandas as pd

def test_unique_constraint():
    """测试唯一约束是否正常工作"""
    print("=" * 50)
    print("开始测试收支明细表唯一约束修复")
    print("=" * 50)
    
    # 初始化数据库管理器
    db_manager = DatabaseManager(SQLITE_CONFIG)
    
    try:
        # 1. 检查数据库配置
        print("\n1. 检查唯一约束配置...")
        print(f"配置文件中的主键字段: {TRANSACTIONS_PRIMARY_KEYS}")
        print(f"字段数量: {len(TRANSACTIONS_PRIMARY_KEYS)}")
        
        # 2. 获取表结构信息
        conn = db_manager.get_connection()
        cursor = conn.cursor()
        
        # 查看表的索引信息
        print("\n2. 检查数据库表的索引信息...")
        cursor.execute("PRAGMA index_list(transactions)")
        indexes = cursor.fetchall()
        
        unique_constraint_found = False
        for index in indexes:
            if index[2] == 1:  # unique index
                print(f"\n找到唯一索引: {index[1]}")
                cursor.execute(f"PRAGMA index_info({index[1]})")
                index_cols = cursor.fetchall()
                
                # 获取字段名
                col_names = []
                for col_info in index_cols:
                    cursor.execute("PRAGMA table_info(transactions)")
                    table_cols = cursor.fetchall()
                    col_names.append(table_cols[col_info[1]][1])
                
                print(f"索引包含的字段: {col_names}")
                print(f"字段数量: {len(col_names)}")
                
                if len(col_names) == 5:
                    unique_constraint_found = True
                    print("✓ 找到5字段唯一约束")
        
        if not unique_constraint_found:
            print("✗ 未找到5字段唯一约束")
            return False
        
        # 3. 插入测试数据
        print("\n3. 测试插入数据...")
        
        # 准备测试数据
        test_data = {
            "fundid": "TEST001",
            "transactiondate": "2025-01-01",
            "vouchernumber": "V001",
            "summary": "测试记录",
            "subjectcode": "1001",
            "subjectname": "测试科目",
            "debitamount": 100.0,
            "creditamount": 0.0,
            "balance": 1000.0,
            "projectname": "测试项目"
        }
        
        # 首次插入 - 应该成功
        try:
            cursor.execute("""
                INSERT INTO transactions (
                    fundid, transactiondate, vouchernumber, summary,
                    subjectcode, subjectname, debitamount, creditamount,
                    balance, projectname
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                test_data["fundid"], test_data["transactiondate"],
                test_data["vouchernumber"], test_data["summary"],
                test_data["subjectcode"], test_data["subjectname"],
                test_data["debitamount"], test_data["creditamount"],
                test_data["balance"], test_data["projectname"]
            ))
            conn.commit()
            print("✓ 首次插入成功")
        except Exception as e:
            print(f"✗ 首次插入失败: {str(e)}")
            return False
        
        # 尝试插入重复数据（5个主键字段都相同）- 应该失败
        print("\n4. 测试插入完全重复的数据（5个主键字段都相同）...")
        try:
            cursor.execute("""
                INSERT INTO transactions (
                    fundid, transactiondate, vouchernumber, summary,
                    subjectcode, subjectname, debitamount, creditamount,
                    balance, projectname
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                test_data["fundid"], test_data["transactiondate"],
                test_data["vouchernumber"], test_data["summary"],
                test_data["subjectcode"], test_data["subjectname"],
                test_data["debitamount"], test_data["creditamount"],
                test_data["balance"], test_data["projectname"]
            ))
            conn.commit()
            print("✗ 插入重复数据成功（不应该成功）")
            return False
        except Exception as e:
            if "UNIQUE constraint failed" in str(e):
                print("✓ 正确拒绝了重复数据")
            else:
                print(f"✗ 出现了其他错误: {str(e)}")
                return False
        
        # 测试只有部分字段不同的情况 - 应该成功
        print("\n5. 测试插入部分字段不同的数据...")
        
        # 改变debitamount，其他保持相同
        test_data2 = test_data.copy()
        test_data2["debitamount"] = 200.0  # 改变借方金额
        
        try:
            cursor.execute("""
                INSERT INTO transactions (
                    fundid, transactiondate, vouchernumber, summary,
                    subjectcode, subjectname, debitamount, creditamount,
                    balance, projectname
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                test_data2["fundid"], test_data2["transactiondate"],
                test_data2["vouchernumber"], test_data2["summary"],
                test_data2["subjectcode"], test_data2["subjectname"],
                test_data2["debitamount"], test_data2["creditamount"],
                test_data2["balance"], test_data2["projectname"]
            ))
            conn.commit()
            print("✓ 成功插入debitamount不同的记录")
        except Exception as e:
            print(f"✗ 插入失败: {str(e)}")
            return False
        
        # 改变balance，其他保持相同
        test_data3 = test_data.copy()
        test_data3["balance"] = 2000.0  # 改变余额
        
        try:
            cursor.execute("""
                INSERT INTO transactions (
                    fundid, transactiondate, vouchernumber, summary,
                    subjectcode, subjectname, debitamount, creditamount,
                    balance, projectname
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                test_data3["fundid"], test_data3["transactiondate"],
                test_data3["vouchernumber"], test_data3["summary"],
                test_data3["subjectcode"], test_data3["subjectname"],
                test_data3["debitamount"], test_data3["creditamount"],
                test_data3["balance"], test_data3["projectname"]
            ))
            conn.commit()
            print("✓ 成功插入balance不同的记录")
        except Exception as e:
            print(f"✗ 插入失败: {str(e)}")
            return False
        
        # 清理测试数据
        print("\n6. 清理测试数据...")
        cursor.execute("DELETE FROM transactions WHERE fundid = 'TEST001'")
        conn.commit()
        print("✓ 测试数据已清理")
        
        print("\n" + "=" * 50)
        print("测试完成：唯一约束修复成功！")
        print("=" * 50)
        return True
        
    except Exception as e:
        print(f"\n✗ 测试过程中出现错误: {str(e)}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        db_manager.close_connection()

def test_with_real_data():
    """使用真实数据测试"""
    print("\n\n测试真实数据处理...")
    print("=" * 50)
    
    from transactions_processor import TransactionsProcessor
    
    # 创建处理器实例
    processor = TransactionsProcessor()
    
    # 准备测试数据
    test_df = pd.DataFrame([
        {
            "fundid": "3932001",
            "transactiondate": "2025-01-01",
            "vouchernumber": "V20250101001",
            "summary": "测试摘要1",
            "subjectcode": "1001",
            "subjectname": "科目1",
            "debitamount": 100.0,
            "creditamount": 0.0,
            "balance": 1000.0,
            "projectname": "测试项目1"
        },
        {
            "fundid": "3932001",
            "transactiondate": "2025-01-01",
            "vouchernumber": "V20250101001",
            "summary": "测试摘要1",
            "subjectcode": "1001",
            "subjectname": "科目1",
            "debitamount": 100.0,  # 完全相同的记录
            "creditamount": 0.0,
            "balance": 1000.0,
            "projectname": "测试项目1"
        },
        {
            "fundid": "3932001",
            "transactiondate": "2025-01-01",
            "vouchernumber": "V20250101001",
            "summary": "测试摘要2",
            "subjectcode": "1002",
            "subjectname": "科目2",
            "debitamount": 200.0,  # debitamount不同
            "creditamount": 0.0,
            "balance": 1000.0,
            "projectname": "测试项目2"
        }
    ])
    
    try:
        # 保存数据
        inserted, updated = processor.database_manager.save_transactions_data(test_df)
        print(f"插入记录数: {len(inserted)}")
        print(f"更新记录数: {len(updated)}")
        
        # 清理测试数据
        conn = processor.database_manager.get_connection()
        cursor = conn.cursor()
        cursor.execute("DELETE FROM transactions WHERE fundid = '3932001' AND transactiondate = '2025-01-01'")
        conn.commit()
        print("✓ 真实数据测试完成")
        
    except Exception as e:
        print(f"✗ 真实数据测试失败: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    # 运行测试
    success = test_unique_constraint()
    
    if success:
        test_with_real_data()
        print("\n所有测试通过！收支明细表的唯一约束已正确配置为5个字段。")
    else:
        print("\n测试失败！请检查数据库结构。")