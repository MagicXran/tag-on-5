"""
测试拆分后的合同数据是否正确传递给OA
"""

import asyncio
import pandas as pd
import sys
import os
from datetime import datetime

# 将当前目录加入系统路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from main_processor import RMSTagOnProcessor
from data_cleaner import DataCleaner
from logger_utils import process_logger


def create_test_excel_file():
    """创建测试Excel文件"""
    test_data = pd.DataFrame([
        {
            "合同编号": "OA-TEST-001",
            "合同名称": "OA同步测试合同1",
            "负责人": "测试人员1",
            "合同经费": 100.0,
            "经费卡号": "3932001,3932002,3932003",  # 3个经费号
            "所属单位": "工程技术研究院",
            "签订日期": "2025-01-10",
            "合同分类": "横向合同",
            "进行状态": "进行中",
            "甲方名称": "测试甲方公司"
        },
        {
            "合同编号": "OA-TEST-002",
            "合同名称": "OA同步测试合同2",
            "负责人": "测试人员2",
            "合同经费": 200.0,
            "经费卡号": "3932004",  # 1个经费号
            "所属单位": "冶金工程研究院",
            "签订日期": "2025-01-11",
            "合同分类": "纵向合同",
            "进行状态": "已完成",
            "甲方名称": "测试甲方机构"
        }
    ])
    
    # 保存到临时文件
    test_file = "test_oa_sync_contracts.xls"
    test_data.to_excel(test_file, index=False, engine='xlwt')
    return test_file


async def test_oa_sync_with_split():
    """测试拆分后的数据OA同步"""
    print("="*60)
    print("测试拆分后的合同数据OA同步")
    print("="*60)
    
    # 创建测试文件
    test_file = create_test_excel_file()
    
    try:
        # 初始化处理器
        processor = RMSTagOnProcessor()
        
        # 1. 处理Excel文件（包含拆分）
        print("\n1. 处理Excel文件...")
        inserted_records, updated_records = processor.process_contracts_excel(test_file)
        
        print(f"\n处理结果：")
        print(f"  插入记录数: {len(inserted_records)}")
        print(f"  更新记录数: {len(updated_records)}")
        
        # 检查拆分结果
        print("\n2. 验证拆分结果...")
        print("\n插入的记录详情：")
        for i, record in enumerate(inserted_records):
            data = record.get('data', {})
            print(f"\n  记录 {i+1}:")
            print(f"    合同编号: {data.get('contractid')}")
            print(f"    合同名称: {data.get('description')}")
            print(f"    经费号: {data.get('fundids')}")
            print(f"    负责人: {data.get('leader')}")
        
        # 验证拆分是否成功
        oa_test_001_records = [r for r in inserted_records if r['data'].get('contractid') == 'OA-TEST-001']
        oa_test_002_records = [r for r in inserted_records if r['data'].get('contractid') == 'OA-TEST-002']
        
        print(f"\n拆分验证：")
        print(f"  OA-TEST-001 拆分成 {len(oa_test_001_records)} 条记录（预期：3条）")
        print(f"  OA-TEST-002 拆分成 {len(oa_test_002_records)} 条记录（预期：1条）")
        
        # 3. 测试OA同步（模拟）
        print("\n3. 准备OA同步数据...")
        print("\n将要同步到OA的数据：")
        
        for record in inserted_records:
            data = record.get('data', {})
            print(f"\n  合同: {data.get('contractid')}, 经费号: {data.get('fundids')}")
            print(f"    - 这是一条独立的OA记录")
            
            # 验证必要字段是否存在
            required_fields = ['contractid', 'description', 'fundids', 'leader']
            missing_fields = [f for f in required_fields if not data.get(f)]
            if missing_fields:
                print(f"    - 警告：缺少字段 {missing_fields}")
            else:
                print(f"    - 所有必要字段都存在")
        
        # 4. 实际同步到OA（可选）
        sync_to_oa = input("\n是否执行实际的OA同步？(y/n): ")
        if sync_to_oa.lower() == 'y':
            print("\n4. 执行OA同步...")
            sync_result = await processor.sync_contracts_to_oa(inserted_records, updated_records)
            
            if sync_result:
                print(f"\nOA同步结果：")
                print(f"  成功插入: {len(sync_result.get('insert_ids', []))} 条")
                print(f"  成功更新: {len(sync_result.get('update_ids', []))} 条")
                print(f"  插入IDs: {sync_result.get('insert_ids', [])}")
            else:
                print("\nOA同步失败或被跳过")
        
        # 清理测试数据
        print("\n5. 清理测试数据...")
        conn = processor.database_manager.get_connection()
        cursor = conn.cursor()
        cursor.execute("DELETE FROM contracts WHERE contractid LIKE 'OA-TEST-%'")
        conn.commit()
        print("  已清理测试数据")
        
    except Exception as e:
        print(f"\n错误: {str(e)}")
        import traceback
        traceback.print_exc()
    
    finally:
        # 删除测试文件
        if os.path.exists(test_file):
            os.remove(test_file)
            print(f"\n已删除测试文件: {test_file}")
        
        # 关闭数据库连接
        if 'processor' in locals():
            processor.database_manager.close_connection()


if __name__ == "__main__":
    # 运行测试
    asyncio.run(test_oa_sync_with_split())
    print("\n测试完成！")