"""
系统配置模块 - 集中管理所有配置信息
"""

import os
import pandas as pd
from datetime import datetime
from typing import Dict, List, Any, Callable

# Excel文件路径配置
EXCEL_FILES = {
    "contracts": r"D:\Nercar\RMS\rms-tag-on\最新数据\合同签订清单_横向_20250425_田文月给.xls",
    "project_funds": r"D:\Nercar\RMS\rms-tag-on\最新数据\经费到账清单_20250101-20250703.xls",
    "transactions": r"D:\Nercar\RMS\rms-tag-on\最新数据\39320283.xls"  # 收支明细表文件夹路径
}

# 数据库类型配置 ("mysql" 或 "sqlite")
DATABASE_TYPE = "sqlite"
# DATABASE_TYPE = "mysql"

# MySQL数据库配置
MYSQL_CONFIG = {
    "user": "root",
    "password": "rms", 
    "host": "localhost",
    "database": "rms",
    "charset": "utf8mb4",
    "port": 3306
}

# SQLite数据库配置
SQLITE_CONFIG = {
    "database": "data/rms.db"  # SQLite数据库文件路径
}

# 获取当前数据库配置
def get_database_config():
    """根据数据库类型返回相应的配置"""
    if DATABASE_TYPE.lower() == "sqlite":
        return SQLITE_CONFIG
    else:
        return MYSQL_CONFIG

# OA系统配置
# OA_CONFIG = {
#     "base_url": "http://10.0.3.39",
#     "login_name": "admin",
#     "rest_user": "beike", 
#     "rest_pass": "de900bd7-dd69-4002-83e1-88e5acfe3ccd",
#     "timeout": 30,
#     "max_retries": 3
# }

# 正式环境
OA_CONFIG = {
    "base_url": "http://222.28.53.189:8089",
    "login_name": "王紫薇",
    "rest_user": "keyanzhanghao", 
    "rest_pass": "1c270a3d-34db-4c61-a572-65a0c9bc8110",
    "timeout": 30,
    "max_retries": 3
}

# 数据类型转换器配置
def safe_str_convert(value) -> str:
    """安全字符串转换"""
    if value is None or (isinstance(value, float) and str(value).lower() == 'nan'):
        return ""
    
    # 对于数字类型，特殊处理避免科学计数法
    if isinstance(value, (int, float)):
        # 如果是整数或者小数点后全为0的浮点数，转为整数字符串
        if isinstance(value, float) and value.is_integer():
            return str(int(value))
        else:
            return str(value)
    
    return str(value).strip()

def safe_fundid_convert(value) -> str:
    """安全的fundid转换，保持原始格式不变"""
    if pd.isna(value) or value is None:
        return ""
    
    # 直接转换为字符串，保持原始格式
    return str(value).strip()

def safe_int_convert(value) -> int:
    """安全整数转换"""
    if value is None or (isinstance(value, float) and str(value).lower() == 'nan'):
        return 0
    try:
        if isinstance(value, str):
            value = value.replace(',', '').strip()
        return int(float(value))
    except (ValueError, TypeError):
        return 0

def safe_float_convert(value) -> float:
    """安全浮点数转换"""
    if value is None or (isinstance(value, float) and str(value).lower() == 'nan'):
        return 0.0
    try:
        if isinstance(value, str):
            value = value.replace(',', '').strip()
        return float(value)
    except (ValueError, TypeError):
        return 0.0

def safe_datetime_convert(value) -> str:
    """安全日期时间转换"""
    if value is None or (isinstance(value, float) and str(value).lower() == 'nan'):
        return ""
    
    try:
        if isinstance(value, datetime):
            return value.strftime('%Y-%m-%d')
        elif isinstance(value, str):
            # 尝试解析各种日期格式
            value = value.strip()
            if not value:
                return ""
            # 这里可以添加更多日期格式解析逻辑
            return value
        else:
            return str(value)
    except (ValueError, TypeError):
        return ""

# 数据类型转换器映射
TYPE_CONVERTERS: Dict[str, Callable] = {
    "str": safe_str_convert,
    "int": safe_int_convert, 
    "float": safe_float_convert,
    "datetime": safe_datetime_convert,
    "fundid": safe_fundid_convert  # 新增：专用于fundid字段的转换器
}

def get_excel_converters(table_type: str) -> Dict[str, Callable]:
    """根据表类型获取Excel读取时的字段转换器
    
    Args:
        table_type: 表类型 ('contracts', 'project_funds', 'transactions')
        
    Returns:
        Dict[str, Callable]: 字段名到转换函数的映射
    """
    # 获取字段类型配置
    if table_type == "contracts":
        field_types = CONTRACTS_FIELD_TYPES
    elif table_type == "project_funds":
        field_types = PROJECT_FUNDS_FIELD_TYPES
    elif table_type == "transactions":
        field_types = TRANSACTIONS_FIELD_TYPES
    else:
        raise ValueError(f"不支持的表类型: {table_type}")
    
    # 生成converters映射
    converters = {}
    for field_name, field_type in field_types.items():
        if field_type in TYPE_CONVERTERS:
            converters[field_name] = TYPE_CONVERTERS[field_type]
        else:
            # 对于未定义的类型，默认使用字符串转换
            converters[field_name] = str
    
    return converters

# 合同数据字段类型配置
CONTRACTS_FIELD_TYPES = {
    "经费卡号": "str",
    "合同编号": "str", 
    "合同名称": "str",
    "负责人": "str",
    "合同经费": "float",
    "合同分类": "str",
    "生效日期": "datetime",
    "所属单位": "str",
    "项目成员": "str",
    "登记号": "str",
    "开始日期": "datetime",
    "终止日期": "datetime",
    "负责人类型": "str",
    "负责人电话": "str",
    "负责人邮箱": "str",
    "经办人": "str",
    "经办人电话": "str",
    "合同有效状态": "str",
    "进行状态": "str",
    "支付方式": "str",
    "甲方是否盖章": "str",
    "乙方是否盖章": "str",
    "合同是否回收": "str",
    "统计归属": "str",
    "一级学科": "str",
    "研究类别": "str",
    "合作形式": "str",
    "项目来源": "str",
    "社会经济服务目标": "str",
    "国民经济行业": "str",
    "审核状态": "str",
    "备注": "str",
    "合同是否生效": "str",
    "甲方名称": "str",
    "甲方类型": "str",
    "甲方联系人": "str",
    "甲方联系电话": "str", 
    "甲方所属省份": "str",
    "甲方所属地市": "str",
    "甲方地址": "str",
    "甲方邮编": "str",
    "专利件数": "int",
    "到账金额": "float",
    "专利(软著)号": "str",
    "负责人职工号": "str",
    "软著件数": "int",
    "转化类型": "str",
    "许可类型": "str",
    "生效日期": "datetime",
    "部门": "str"
}

# 经费数据字段类型配置  
PROJECT_FUNDS_FIELD_TYPES = {
    "项目名称": "str",
    "经费卡负责人": "str",
    "入账经费（万元）": "float",
    "经费账号": "fundid",
    "拨款时间": "datetime",
    "项目所属单位": "str",
    "项目编号": "str",
    "批准(合同)经费【万元】": "float",
    "经费卡负责人工号": "str",
    "项目负责人": "str",
    "所在单位": "str",
    "项目分类": "str",
    "财务回单编号": "str",
    "留校金额（万元）": "float",
    "外拨金额": "float",
    "来款单位": "str",
    "来款类型": "str",
    "审核状态": "str",
    "项目性质": "str",
    "项目级别": "str"
}

# 收支明细表字段类型配置
TRANSACTIONS_FIELD_TYPES = {
    "经费卡号": "fundid",
    "日期": "datetime",
    "凭证号": "str",
    "摘要": "str",
    "科目代码": "str",
    "科目名称": "str",
    "借方金额": "float",
    "贷方金额": "float",
    "余额": "float",
    "期末余额": "float",
    "借方累计发生额": "float",
    "贷方累计发生额": "float",
    "项目名称": "str",
    "序号1": "int",
    "期初余额": "float"
}

# 列名映射配置（中文->英文）
CONTRACTS_COLUMN_MAPPING = {
    "合同编号": "contractid",
    "合同名称": "description", 
    "负责人": "leader",
    "合同经费": "contractfunds",
    "合同分类": "contractclassification",
    "生效日期": "signdate",
    "所属单位": "undertakingunit",
    "项目成员": "projectmembers",
    "登记号": "registrationid",
    "开始日期": "startdate",
    "终止日期": "enddate",
    "负责人类型": "leadertype",
    "负责人电话": "leadertelephone",
    "负责人邮箱": "leaderemail",
    "经办人": "operator",
    "经办人电话": "operatortelephone", 
    "合同有效状态": "contracteffectivestatus",
    "进行状态": "contractstatus",
    "支付方式": "paymentmode",
    "甲方是否盖章": "partyaseal",
    "乙方是否盖章": "partybseal",
    "合同是否回收": "contractrecovered",
    "统计归属": "statisticalattribution",
    "一级学科": "subjectclassification",
    "研究类别": "researchcategory",
    "合作形式": "formscooperation",
    "项目来源": "projectsource",
    "社会经济服务目标": "socialeconomictarget",
    "国民经济行业": "neic",
    "审核状态": "auditstatus",
    "备注": "remarks",
    "合同是否生效": "iseffective",
    "甲方名称": "partyaname",
    "甲方类型": "partyatype",
    "甲方联系人": "partyacontact",
    "甲方联系电话": "partyatel", 
    "甲方所属省份": "partaprovince",
    "甲方所属地市": "partyacity",
    "甲方地址": "partaaddress",
    "甲方邮编": "partapostalcode",
    "专利件数": "patentcount",
    "到账金额": "amountreceived",
    "专利(软著)号": "copyrightid",
    "负责人职工号": "manageremployeeid",
    "软著件数": "copyrightcount",
    "经费卡号": "fundids",
    "转化类型": "conversiontype",
    "许可类型": "licensetype",
    "生效日期": "effective_date",
    "部门": "department"
}

PROJECT_FUNDS_COLUMN_MAPPING = {
    "项目名称": "project_name",
    "经费卡负责人": "fund_manager",
    "入账经费（万元）": "funds_received",
    "经费账号": "fundid",
    "拨款时间": "allocation_date",
    "项目所属单位": "project_unit",
    "项目编号": "contractid",
    "批准(合同)经费【万元】": "approved_funds",
    "经费卡负责人工号": "manager_id",
    "项目负责人": "project_leader",
    "所在单位": "fund_unit",
    "项目分类": "project_category",
    "财务回单编号": "receipt_id",
    "留校金额（万元）": "retained_funds",
    "外拨金额": "allocated_funds",
    "来款单位": "payment_unit",
    "来款类型": "payment_type",
    "审核状态": "audit_status",
    "项目性质": "project_nature",
    "项目级别": "project_level",
    "单据号": "unid"  # 系统自动生成UUID，不来自Excel
}

# 收支明细表列名映射配置
TRANSACTIONS_COLUMN_MAPPING = {
    "经费卡号": "fundid",
    "日期": "transactiondate",
    "凭证号": "vouchernumber",
    "摘要": "summary",
    "科目代码": "subjectcode",
    "科目名称": "subjectname",
    "借方金额": "debitamount",
    "贷方金额": "creditamount",
    "余额": "balance",
    "期末余额": "endingbalance",
    "借方累计发生额": "totaldebit",
    "贷方累计发生额": "totalcredit",
    "项目名称": "projectname",
    "序号1": "sequencenumber",
    "期初余额": "openingbalance"
}

# 业务筛选条件配置
BUSINESS_FILTER_CONFIG = {
    "contracts": {
        # 所属单位包含列表
        "include_units": ["工程技术研究院", "冶金工程研究院"],
        # 经费卡号前缀白名单
        "fund_id_prefixes": ["3932", "3832", "3934"],
        # 必须非空的字段
        "required_fields": ["合同编号"]
    },
    "project_funds": {
        # 项目所属单位和所在单位筛选
        "include_units": ["工程技术研究院", "冶金工程研究院"], 
        # 必须非空的字段
        "required_fields": ["经费账号", "入账经费（万元）", "项目编号"]
    }
}

# OA字段映射配置
OA_FIELD_MAPPING = {
    "contracts": {
        "field0001": "合同编号", "field0002": "合同名称", "field0003": "负责人",
        "field0004": "合同经费", "field0005": "合同分类", "field0006": "生效日期",
        "field0007": "所属单位", "field0008": "项目成员", "field0009": "登记号",
        "field0010": "开始日期", "field0011": "终止日期", "field0012": "负责人类型",
        "field0013": "负责人电话", "field0014": "负责人邮箱", "field0015": "经办人",
        "field0016": "经办人电话", "field0017": "合同有效状态", "field0018": "进行状态",
        "field0019": "支付方式", "field0020": "甲方是否盖章", "field0021": "乙方是否盖章",
        "field0022": "合同是否回收", "field0023": "统计归属", "field0024": "一级学科",
        "field0025": "研究类别", "field0026": "合作形式", "field0027": "项目来源",
        "field0028": "社会经济服务目标", "field0029": "国民经济行业", "field0030": "审核状态1",
        "field0031": "备注", "field0032": "合同是否生效", "field0033": "甲方名称",
        "field0034": "甲方联系人", "field0035": "甲方联系电话", "field0036": "甲方类型",
        "field0037": "甲方所属省份", "field0038": "甲方所属地市", "field0039": "甲方邮编",
        "field0040": "甲方地址", "field0041": "专利件数", "field0042": "到账金额",
        "field0043": "专利软著号", "field0044": "负责人职工号", "field0045": "软著件数",
        "field0046": "经费卡号", "field0047": "项目所属部门", "field0048": "转化类型",
        "field0049": "许可类型", "field0050": "部门"
    },
    "project_funds": {
        "field0001": "项目名称", "field0002": "经费卡号", "field0003": "经费卡负责人",
        "field0004": "入账经费", "field0005": "拨款日期", "field0006": "项目所属单位",
        "field0007": "项目编号", "field0008": "批准合同经费", "field0009": "经费卡负责人工号",
        "field0010": "项目负责人", "field0011": "经费所在单位", "field0012": "项目分类",
        "field0013": "财务回单编号", "field0014": "留校金额", "field0015": "外拨金额",
        "field0016": "来款单位", "field0017": "来款类型", "field0018": "审核状态1",
        "field0019": "项目性质", "field0020": "项目级别","field0024": "单据号"
    },
    "transactions": {
        "field0001": "经费卡号", "field0002": "项目名称", "field0003": "借方累计发生额",
        "field0004": "贷方累计发生额", "field0005": "期末余额", "field0016": "期初余额",
        "field0006": "序号1", "field0007": "日期", "field0008": "凭证号",
        "field0009": "摘要", "field0010": "科目代码", "field0011": "科目名称",
        "field0012": "借方金额", "field0013": "贷方金额", "field0014": "余额",
        "field0015": "项目编号"
    }
}

# OA表单配置
OA_TABLE_CONFIG = {
    "contracts": {
        "masterTable": "formmain_0016",
        "subTable": None,
        "rightId": "8738386221147704578.4686476435574376301",
        "formCode": "hetongdangan"
    },
    "project_funds": {
        "masterTable": "formmain_0017", 
        "subTable": None,
        "rightId": "-8468617479218967734.-5627575296880786341",
        "formCode": "jingfeidaozhangqingdan"
    },
    "transactions": {
        "masterTable": "formmain_0018",
        "subTable": "formson_0019",
        "rightId": "-5159805959137505715.-6609998234064302859",
        "formCode": "shouzhimingxi",
        "groupKey": "fundid",
        "excludeFields": ["fundid", "transname"]
    }
}

# 日志配置
LOG_CONFIG = {
    "log_dir": "logs",
    "log_level": "INFO",
    "max_file_size": 10 * 1024 * 1024,  # 10MB
    "backup_count": 5
}

# 运行时配置
RUNTIME_CONFIG = {
    "enable_oa_sync": True,  # 是否启用OA同步
    "enable_oa_master_sub_table": True,  # 是否启用OA主从表
    "batch_size": 100,       # 批处理大小
    "max_workers": 5,        # 最大工作线程数
    "connection_pool_size": 10  # 数据库连接池大小
}

# 收支明细表Excel解析常量
TRANSACTIONS_EXCEL_CONSTANTS = {
    "HEADER_ROW": 2,           # 经费卡号信息所在行（第2行）
    "HEADER_COLUMN_START": 2,  # 开始列C (0-based为2)
    "HEADER_COLUMN_END": 7,    # 结束列H (0-based为7)
    "FIELD_ROW": 3,           # 字段名所在行（第3行）
    "DATA_START_ROW": 5,      # 数据开始行（第5行）
    "SKIP_ROW": 4,            # 需要跳过的行（第4行）
    "SUMMARY_KEYWORDS": ["累计发生额", "期末余额"],  # 汇总行关键词
    "FIELD_NAMES": ["日期", "凭证号", "摘要", "科目代码", "科目名称", "借方金额", "贷方金额", "余额"]
}

# 数据库表主键配置
DATABASE_PRIMARY_KEYS = {
    "contracts": ["contractid"],  # 合同表主键
    "projectfunds": ["unid"],  # 经费表主键：单据号（系统自动生成UUID，每条记录唯一）
    "transactions": ["fundid", "transactiondate", "vouchernumber", "debitamount", "balance"]  # 收支明细表联合主键
}

# 收支表主键（保留向后兼容性）
TRANSACTIONS_PRIMARY_KEYS = DATABASE_PRIMARY_KEYS["transactions"]