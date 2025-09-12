"""
测试部门信息匹配功能
"""

import pandas as pd
import os
import sys
import tempfile
import shutil

# 将当前目录加入系统路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from data_cleaner import DataCleaner
from database_manager_sqlite import DatabaseManager
from main_processor import RMSTagOnProcessor
from config import get_database_config
from logger_utils import process_logger


def create_test_personnel_file(test_dir):
    """创建测试用的人员名单文件"""
    personnel_data = pd.DataFrame([
        {"人员编号": "EMP001", "姓名": "张三", "部门": "工程技术研究院"},
        {"人员编号": "EMP002", "姓名": "李四", "部门": "冶金工程研究院"},
        {"人员编号": "EMP003", "姓名": "王五", "部门": "材料科学研究院"},
        {"人员编号": "EMP004", "姓名": "赵六", "部门": "信息技术研究院"},
        {"人员编号": "EMP005", "姓名": "钱七", "部门": "工程技术研究院"},
    ])
    
    personnel_file = os.path.join(test_dir, "学校人员名单.xlsx")
    personnel_data.to_excel(personnel_file, index=False)
    return personnel_file


def create_test_contracts_file(test_dir):
    """创建测试用的合同清单文件"""
    contracts_data = pd.DataFrame([
        {
            "合同编号": "HT-DEPT-001",
            "合同名称": "测试合同1-有部门",
            "负责人": "张三",
            "负责人职工号": "EMP001",
            "合同经费": 100.0,
            "经费卡号": "3932001",
            "所属单位": "工程技术研究院",
            "签订日期": "2025-01-01"
        },
        {
            "合同编号": "HT-DEPT-002",
            "合同名称": "测试合同2-有部门",
            "负责人": "李四",
            "负责人职工号": "EMP002",
            "合同经费": 200.0,
            "经费卡号": "3932002,3932003",
            "所属单位": "冶金工程研究院",
            "签订日期": "2025-01-02"
        },
        {
            "合同编号": "HT-DEPT-003",
            "合同名称": "测试合同3-无部门匹配",
            "负责人": "未知人员",
            "负责人职工号": "EMP999",
            "合同经费": 300.0,
            "经费卡号": "3932004",
            "所属单位": "工程技术研究院",
            "签订日期": "2025-01-03"
        },
        {
            "合同编号": "HT-DEPT-004",
            "合同名称": "测试合同4-无职工号",
            "负责人": "无编号人员",
            "负责人职工号": "",
            "合同经费": 400.0,
            "经费卡号": "3932005",
            "所属单位": "冶金工程研究院",
            "签订日期": "2025-01-04"
        }
    ])
    
    contracts_file = os.path.join(test_dir, "合同签订清单_测试.xls")
    contracts_data.to_excel(contracts_file, index=False, engine='xlwt')
    return contracts_file


def test_personnel_list_loading():
    """测试人员名单加载功能"""
    print("="*60)
    print("测试人员名单加载功能")
    print("="*60)
    
    # 创建临时测试目录
    test_dir = tempfile.mkdtemp()
    
    try:
        # 创建测试文件
        personnel_file = create_test_personnel_file(test_dir)
        
        # 初始化数据清洗器
        data_cleaner = DataCleaner()
        
        # 测试查找人员名单文件
        found_file = data_cleaner.find_personnel_file(test_dir)
        print(f"\n找到人员名单文件: {found_file}")
        assert found_file == personnel_file
        
        # 测试加载人员名单
        personnel_dept_map = data_cleaner.load_personnel_list(test_dir)
        print(f"\n加载的人员部门映射:")
        for emp_id, dept in personnel_dept_map.items():
            print(f"  {emp_id}: {dept}")
        
        # 验证数据
        assert len(personnel_dept_map) == 5
        assert personnel_dept_map["EMP001"] == "工程技术研究院"
        assert personnel_dept_map["EMP002"] == "冶金工程研究院"
        
        print("\n人员名单加载测试通过！")
        
    finally:
        # 清理测试目录
        shutil.rmtree(test_dir)


def test_department_matching():
    """测试部门信息匹配功能"""
    print("\n" + "="*60)
    print("测试部门信息匹配功能")
    print("="*60)
    
    # 创建临时测试目录
    test_dir = tempfile.mkdtemp()
    
    try:
        # 创建测试文件
        personnel_file = create_test_personnel_file(test_dir)
        contracts_file = create_test_contracts_file(test_dir)
        
        # 初始化处理器
        processor = RMSTagOnProcessor()
        
        # 测试处理合同文件夹
        print("\n处理合同文件夹...")
        results = processor.process_contracts_folder(test_dir)
        
        # 检查结果
        if not results.get('success', True):
            print(f"处理失败: {results.get('message')}")
            return
        
        print(f"\n处理结果:")
        print(f"  处理文件数: {results['processed_files']}")
        print(f"  总插入数: {results['total_inserted']}")
        print(f"  总更新数: {results['total_updated']}")
        
        # 验证部门信息
        print("\n验证部门信息匹配结果:")
        inserted_records = results.get('all_inserted_records', [])
        
        for record in inserted_records:
            data = record.get('data', {})
            contractid = data.get('contractid')
            manager_id = data.get('manageremployeeid')
            department = data.get('department')
            
            print(f"\n合同: {contractid}")
            print(f"  负责人职工号: {manager_id}")
            print(f"  部门: {department}")
            
            # 验证匹配结果
            if contractid == "HT-DEPT-001":
                assert department == "工程技术研究院", f"部门匹配错误: {department}"
            elif contractid == "HT-DEPT-002":
                assert department == "冶金工程研究院", f"部门匹配错误: {department}"
            elif contractid == "HT-DEPT-003":
                assert department == "", f"无匹配应为空: {department}"
            elif contractid == "HT-DEPT-004":
                assert department == "", f"无职工号应为空: {department}"
        
        print("\n部门信息匹配测试通过！")
        
        # 清理测试数据
        conn = processor.database_manager.get_connection()
        cursor = conn.cursor()
        cursor.execute("DELETE FROM contracts WHERE contractid LIKE 'HT-DEPT-%'")
        conn.commit()
        
    finally:
        # 清理测试目录
        shutil.rmtree(test_dir)
        # 关闭数据库连接
        if 'processor' in locals():
            processor.database_manager.close_connection()


def test_missing_personnel_file():
    """测试缺少人员名单文件的情况"""
    print("\n" + "="*60)
    print("测试缺少人员名单文件的情况")
    print("="*60)
    
    # 创建临时测试目录
    test_dir = tempfile.mkdtemp()
    
    try:
        # 只创建合同文件，不创建人员名单文件
        contracts_file = create_test_contracts_file(test_dir)
        
        # 初始化处理器
        processor = RMSTagOnProcessor()
        
        # 测试处理合同文件夹
        print("\n处理合同文件夹（无人员名单）...")
        results = processor.process_contracts_folder(test_dir)
        
        # 检查结果
        assert results.get('success') == False, "应该返回失败状态"
        assert "未找到人员名单文件" in results.get('message', ''), "应该包含正确的错误信息"
        
        print(f"\n正确识别到缺少人员名单文件:")
        print(f"  错误信息: {results.get('message')}")
        
        print("\n缺少人员名单文件测试通过！")
        
    finally:
        # 清理测试目录
        shutil.rmtree(test_dir)
        # 关闭数据库连接
        if 'processor' in locals():
            processor.database_manager.close_connection()


if __name__ == "__main__":
    # 运行测试
    test_personnel_list_loading()
    test_department_matching()
    test_missing_personnel_file()
    
    print("\n" + "="*60)
    print("所有测试完成！")
    print("="*60)