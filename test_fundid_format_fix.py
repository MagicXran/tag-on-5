#!/usr/bin/env python3
"""
测试fundid字段格式修复的效果
验证经费账号字段是否能保持原始格式不变
"""

import pandas as pd
import os
import sys
from typing import Dict, Any

# 添加当前目录到Python路径
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)

from data_cleaner import DataCleaner
from config import safe_fundid_convert, get_excel_converters
from transactions_processor import TransactionsProcessor


class FundIdFormatTester:
    """fundid格式测试器"""
    
    def __init__(self):
        self.data_cleaner = DataCleaner()
        self.test_results = []
    
    def create_test_excel_data(self) -> pd.DataFrame:
        """创建测试用的Excel数据，包含各种fundid格式"""
        test_data = {
            "项目名称": ["测试项目1", "测试项目2", "测试项目3", "测试项目4", "测试项目5"],
            "经费账号": ["39320283", "039320283", "393202830", "39320283.0", "393202"],
            "经费卡负责人": ["张三", "李四", "王五", "赵六", "孙七"],
            "入账经费（万元）": [10.5, 20.0, 15.8, 8.2, 12.6],
            "项目编号": ["P001", "P002", "P003", "P004", "P005"],
            "拨款时间": ["2024-01-15", "2024-02-20", "2024-03-10", "2024-04-05", "2024-05-12"]
        }
        return pd.DataFrame(test_data)
    
    def test_safe_fundid_convert(self) -> Dict[str, Any]:
        """测试safe_fundid_convert函数"""
        print("=" * 50)
        print("测试 safe_fundid_convert 函数")
        print("=" * 50)
        
        test_cases = [
            ("39320283", "39320283"),          # 普通数字字符串
            ("039320283", "039320283"),        # 带前导零的字符串
            (39320283, "39320283"),           # 整数
            (39320283.0, "39320283.0"),       # 浮点数
            ("", ""),                         # 空字符串
            (None, ""),                       # None值
                         (float('nan'), ""),               # pandas NaN
        ]
        
        results = {"passed": 0, "failed": 0, "details": []}
        
        for input_val, expected in test_cases:
            try:
                result = safe_fundid_convert(input_val)
                if result == expected:
                    status = "✅ PASS"
                    results["passed"] += 1
                else:
                    status = "❌ FAIL"
                    results["failed"] += 1
                
                details = f"{status} | 输入: {repr(input_val)} -> 输出: '{result}' | 期望: '{expected}'"
                print(details)
                results["details"].append(details)
                
            except Exception as e:
                status = "❌ ERROR"
                results["failed"] += 1
                details = f"{status} | 输入: {repr(input_val)} -> 错误: {str(e)}"
                print(details)
                results["details"].append(details)
        
        print(f"\n总结: 通过 {results['passed']} 个，失败 {results['failed']} 个")
        return results
    
    def test_excel_reading_with_converters(self) -> Dict[str, Any]:
        """测试使用converters读取Excel是否能保持格式"""
        print("\n" + "=" * 50)
        print("测试 Excel 读取 converters 效果")
        print("=" * 50)
        
        # 创建测试数据
        test_df = self.create_test_excel_data()
        
        # 保存为临时Excel文件
        temp_file = "temp_test_fundid.xlsx"
        test_df.to_excel(temp_file, index=False, engine='openpyxl')
        
        try:
            # 不使用converters读取
            print("不使用converters读取:")
            df_without_converters = pd.read_excel(temp_file, engine='openpyxl')
            print("经费账号列数据类型:", df_without_converters['经费账号'].dtype)
            print("经费账号列数据:", df_without_converters['经费账号'].tolist())
            
            # 使用converters读取
            print("\n使用converters读取:")
            converters = {"经费账号": str, "项目编号": str}
            df_with_converters = pd.read_excel(temp_file, engine='openpyxl', converters=converters)
            print("经费账号列数据类型:", df_with_converters['经费账号'].dtype)
            print("经费账号列数据:", df_with_converters['经费账号'].tolist())
            
            # 验证结果
            original_fundids = test_df['经费账号'].tolist()
            converted_fundids = df_with_converters['经费账号'].tolist()
            
            print(f"\n原始数据: {original_fundids}")
            print(f"转换后数据: {converted_fundids}")
            
            # 检查是否保持格式
            format_preserved = True
            for orig, conv in zip(original_fundids, converted_fundids):
                if str(orig) != str(conv):
                    format_preserved = False
                    print(f"❌ 格式变化: {orig} -> {conv}")
            
            if format_preserved:
                print("✅ 所有fundid格式都被正确保持")
            
            return {
                "format_preserved": format_preserved,
                "original": original_fundids,
                "converted": converted_fundids
            }
        
        except Exception as e:
            print(f"❌ 测试出错: {str(e)}")
            return {"error": str(e)}
        
        finally:
            # 清理临时文件
            if os.path.exists(temp_file):
                os.remove(temp_file)
    
    def test_excel_converters_config(self) -> Dict[str, Any]:
        """测试get_excel_converters函数是否正确生成转换器"""
        print("\n" + "=" * 50)
        print("测试 get_excel_converters 配置生成")
        print("=" * 50)
        
        try:
            # 测试三个表的转换器生成
            tables = ["contracts", "project_funds", "transactions"]
            results = {"passed": 0, "failed": 0, "details": []}
            
            for table_type in tables:
                try:
                    converters = get_excel_converters(table_type)
                    
                    # 验证返回的是字典
                    if isinstance(converters, dict):
                        status = "✅ PASS"
                        results["passed"] += 1
                        details = f"{status} | {table_type}表: 生成了{len(converters)}个字段转换器"
                    else:
                        status = "❌ FAIL"
                        results["failed"] += 1
                        details = f"{status} | {table_type}表: 返回类型错误"
                    
                    print(details)
                    results["details"].append(details)
                    
                    # 检查fundid相关字段是否使用了正确的转换器
                    fundid_fields = []
                    for field_name, converter in converters.items():
                        if 'fundid' in converter.__name__ if hasattr(converter, '__name__') else False:
                            fundid_fields.append(field_name)
                    
                    if fundid_fields:
                        print(f"  - fundid相关字段: {fundid_fields}")
                    
                except Exception as e:
                    status = "❌ ERROR"
                    results["failed"] += 1
                    details = f"{status} | {table_type}表: 错误 {str(e)}"
                    print(details)
                    results["details"].append(details)
            
            print(f"\n总结: 通过 {results['passed']} 个，失败 {results['failed']} 个")
            return results
            
        except Exception as e:
            print(f"❌ 配置测试出错: {str(e)}")
            return {"error": str(e)}
    
    def test_data_cleaner_integration(self) -> Dict[str, Any]:
        """测试data_cleaner的完整处理流程"""
        print("\n" + "=" * 50)
        print("测试 DataCleaner 完整流程")
        print("=" * 50)
        
        # 创建测试数据并保存为Excel
        test_df = self.create_test_excel_data()
        temp_file = "temp_test_integration.xlsx"
        test_df.to_excel(temp_file, index=False, engine='openpyxl')
        
        try:
            # 使用data_cleaner处理
            processed_df = self.data_cleaner.process_project_funds_excel(temp_file)
            
            print("处理后的数据:")
            print(processed_df[['fundid']].head())
            
            # 检查fundid字段格式
            fundid_values = processed_df['fundid'].tolist()
            print(f"\nfundid字段值: {fundid_values}")
            
            # 验证是否保持原始格式
            expected_fundids = ["39320283", "039320283", "393202830", "39320283.0", "393202"]
            format_correct = True
            
            for i, (actual, expected) in enumerate(zip(fundid_values, expected_fundids)):
                if str(actual) != expected:
                    print(f"❌ 行{i+1}: 期望'{expected}', 实际'{actual}'")
                    format_correct = False
                else:
                    print(f"✅ 行{i+1}: '{actual}' 格式正确")
            
            return {
                "format_correct": format_correct,
                "processed_data": fundid_values
            }
        
        except Exception as e:
            print(f"❌ 集成测试出错: {str(e)}")
            import traceback
            traceback.print_exc()
            return {"error": str(e)}
        
        finally:
            # 清理临时文件
            if os.path.exists(temp_file):
                os.remove(temp_file)
    
    def run_all_tests(self) -> Dict[str, Any]:
        """运行所有测试"""
        print("开始fundid格式修复测试...")
        print("测试目标：确保fundid字段保持与Excel原始数据完全一致的格式")
        
        results = {
            "safe_fundid_convert": self.test_safe_fundid_convert(),
            "excel_converters": self.test_excel_reading_with_converters(),
            "excel_converters_config": self.test_excel_converters_config(),
            "data_cleaner_integration": self.test_data_cleaner_integration()
        }
        
        print("\n" + "=" * 60)
        print("测试总结")
        print("=" * 60)
        
        all_passed = True
        
        # 检查各项测试结果
        if results["safe_fundid_convert"]["failed"] == 0:
            print("✅ safe_fundid_convert 函数测试通过")
        else:
            print("❌ safe_fundid_convert 函数测试失败")
            all_passed = False
        
        if results["excel_converters"].get("format_preserved", False):
            print("✅ Excel converters 测试通过")
        else:
            print("❌ Excel converters 测试失败")
            all_passed = False
        
        if results["excel_converters_config"]["failed"] == 0:
            print("✅ Excel converters 配置测试通过")
        else:
            print("❌ Excel converters 配置测试失败")
            all_passed = False
        
        if results["data_cleaner_integration"].get("format_correct", False):
            print("✅ DataCleaner 集成测试通过")
        else:
            print("❌ DataCleaner 集成测试失败")
            all_passed = False
        
        if all_passed:
            print("\n🎉 所有测试都通过！fundid格式修复成功！")
        else:
            print("\n⚠️  部分测试失败，需要进一步修复")
        
        return results


def main():
    """主函数"""
    tester = FundIdFormatTester()
    results = tester.run_all_tests()
    
    # 返回测试结果供进一步分析
    return results


if __name__ == "__main__":
    main() 