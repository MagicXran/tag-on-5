"""RMS 数据导入系统配置。"""

from __future__ import annotations

import os
from datetime import date, datetime
from typing import Any, Callable, Dict

import pandas as pd


DATABASE_TYPE = "sqlite"
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
SQLITE_CONFIG = {"database": os.path.join(PROJECT_ROOT, "data", "rms_v2.db")}

EXCEL_FILES = {"contracts": "", "project_funds": "", "transactions": ""}

THIRD_PARTY_CONFIG = {
    "enabled": True,
    "base_url": os.getenv("RMS_THIRD_PARTY_URL", "http://127.0.0.1:8080"),
    "endpoint": "/api/v1/rms/batch-import",
    "batch_size": 100,
    "timeout": 60,
    "retry_interval": 3,
    "max_retries": 3,
}

RUNTIME_CONFIG = {
    "enable_third_party_push": True,
    "batch_size": THIRD_PARTY_CONFIG["batch_size"],
}

LOG_CONFIG = {
    "log_dir": "logs",
    "log_level": "INFO",
    "log_file_format": "rms_tag_on_{date}.log",
    "backup_count": 5,
}


def get_database_config() -> Dict[str, Any]:
    return SQLITE_CONFIG


def safe_str_convert(value: Any) -> str:
    if value is None or pd.isna(value):
        return ""
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value).strip()


def safe_fundid_convert(value: Any) -> str:
    return safe_str_convert(value)


def safe_int_convert(value: Any) -> int:
    if value is None or pd.isna(value) or str(value).strip() == "":
        return 0
    try:
        return int(float(str(value).replace(",", "").strip()))
    except (TypeError, ValueError):
        return 0


def safe_float_convert(value: Any) -> float:
    if value is None or pd.isna(value) or str(value).strip() == "":
        return 0.0
    try:
        return float(str(value).replace(",", "").strip())
    except (TypeError, ValueError):
        return 0.0


def safe_datetime_convert(value: Any) -> str:
    if value is None or pd.isna(value) or str(value).strip() == "":
        return ""
    if isinstance(value, (datetime, date, pd.Timestamp)):
        return value.strftime("%Y-%m-%d")
    parsed = pd.to_datetime(value, errors="coerce")
    if not pd.isna(parsed):
        return parsed.strftime("%Y-%m-%d")
    raise ValueError(f"无效日期: {value}")


TYPE_CONVERTERS: Dict[str, Callable[[Any], Any]] = {
    "str": safe_str_convert,
    "fundid": safe_fundid_convert,
    "int": safe_int_convert,
    "float": safe_float_convert,
    "date": safe_datetime_convert,
    "datetime": safe_datetime_convert,
}


CONTRACTS_COLUMN_MAPPING = {
    "合同编号": "contract_id",
    "合同名称": "contract_name",
    "负责人": "leader",
    "合同经费": "contract_funds",
    "合同分类": "contract_category",
    "生效日期": "effective_date",
    "所属单位": "undertaking_unit",
    "项目成员": "project_members",
    "登记号": "registration_id",
    "开始日期": "start_date",
    "终止日期": "end_date",
    "负责人类型": "leader_type",
    "负责人电话": "leader_phone",
    "负责人邮箱": "leader_email",
    "经办人": "operator",
    "经办人电话": "operator_phone",
    "合同有效状态": "validity_status",
    "进行状态": "progress_status",
    "支付方式": "payment_method",
    "甲方是否盖章": "party_a_sealed",
    "乙方是否盖章": "party_b_sealed",
    "合同是否回收": "contract_recovered",
    "统计归属": "statistical_attribution",
    "一级学科": "primary_discipline",
    "研究类别": "research_category",
    "合作形式": "cooperation_form",
    "项目来源": "project_source",
    "社会经济服务目标": "socioeconomic_target",
    "国民经济行业": "national_economy_industry",
    "审核状态": "audit_status",
    "备注": "remarks",
    "合同是否生效": "is_effective",
    "甲方名称": "party_a_name",
    "甲方联系人": "party_a_contact",
    "甲方联系电话": "party_a_phone",
    "甲方类型": "party_a_type",
    "甲方所属省份": "party_a_province",
    "甲方所属地市": "party_a_city",
    "甲方邮编": "party_a_postal_code",
    "甲方地址": "party_a_address",
    "专利件数": "patent_count",
    "到账金额": "amount_received",
    "专利(软著)号": "intellectual_property_number",
    "负责人职工号": "leader_employee_id",
    "软著件数": "software_copyright_count",
    "经费卡号": "fund_id",
    "转化类型": "conversion_type",
    "许可类型": "license_type",
    "代购经费": "purchase_funds",
    "合作经费": "cooperation_funds",
}

CONTRACTS_EXPECTED_COLUMNS = list(CONTRACTS_COLUMN_MAPPING)
CONTRACTS_FIELD_TYPES = {
    column: (
        "float"
        if column in {"合同经费", "到账金额", "代购经费", "合作经费"}
        else "int"
        if column in {"专利件数", "软著件数"}
        else "date"
        if column in {"生效日期", "开始日期", "终止日期"}
        else "fundid"
        if column == "经费卡号"
        else "str"
    )
    for column in CONTRACTS_EXPECTED_COLUMNS
}


PROJECT_FUNDS_COLUMN_MAPPING = {
    "项目名称": "project_name",
    "经费卡负责人": "fund_manager",
    "入账经费（万元）": "funds_received",
    "经费账号": "fund_id",
    "拨款时间": "allocation_date",
    "项目所属单位": "project_unit",
    "项目编号": "contract_id",
    "批准号": "approval_number",
    "批准(合同)经费【万元】": "approved_funds",
    "经费卡负责人工号": "manager_employee_id",
    "项目负责人": "project_leader",
    "所在单位": "fund_unit",
    "项目分类": "project_category",
    "财务回单编号": "receipt_number",
    "留校金额（万元）": "retained_funds",
    "外拨金额": "allocated_funds",
    "来款单位": "payment_unit",
    "来款类型": "payment_type",
    "审核状态": "audit_status",
    "项目性质": "project_nature",
    "项目级别": "project_level",
}

PROJECT_FUNDS_EXPECTED_COLUMNS = list(PROJECT_FUNDS_COLUMN_MAPPING)
PROJECT_FUNDS_FIELD_TYPES = {
    column: (
        "float"
        if column
        in {
            "入账经费（万元）",
            "批准(合同)经费【万元】",
            "留校金额（万元）",
            "外拨金额",
        }
        else "datetime"
        if column == "拨款时间"
        else "fundid"
        if column == "经费账号"
        else "str"
    )
    for column in PROJECT_FUNDS_EXPECTED_COLUMNS
}


TRANSACTIONS_FIELD_TYPES = {
    "经费卡号": "fundid",
    "日期": "date",
    "凭证号": "str",
    "摘要": "str",
    "科目代码": "str",
    "科目名称": "str",
    "借方金额": "float",
    "贷方金额": "float",
    "余额": "float",
    "序号1": "int",
}

TRANSACTIONS_COLUMN_MAPPING = {
    "经费卡号": "fund_id",
    "日期": "transaction_date",
    "凭证号": "voucher_number",
    "摘要": "summary",
    "科目代码": "subject_code",
    "科目名称": "subject_name",
    "借方金额": "debit_amount",
    "贷方金额": "credit_amount",
    "余额": "balance",
    "序号1": "sequence_number",
}

TRANSACTIONS_EXCEL_CONSTANTS = {
    "HEADER_ROW": 2,
    "HEADER_COLUMN_START": 2,
    "HEADER_COLUMN_END": 7,
    "FIELD_ROW": 3,
    "DATA_START_ROW": 5,
    "FIELD_NAMES": [
        "日期",
        "凭证号",
        "摘要",
        "科目代码",
        "科目名称",
        "借方金额",
        "贷方金额",
        "余额",
    ],
    "SUMMARY_KEYWORDS": ["累计发生额", "期末余额"],
}

DATABASE_PRIMARY_KEYS = {
    "contracts": ["contract_id", "fund_id"],
    "projectfunds": ["unid"],
    "transactions": [
        "fund_id",
        "transaction_date",
        "voucher_number",
        "debit_amount",
        "balance",
    ],
}
TRANSACTIONS_PRIMARY_KEYS = DATABASE_PRIMARY_KEYS["transactions"]


def get_excel_converters(table_type: str) -> Dict[str, Callable[[Any], Any]]:
    field_types = {
        "contracts": CONTRACTS_FIELD_TYPES,
        "project_funds": PROJECT_FUNDS_FIELD_TYPES,
        "transactions": TRANSACTIONS_FIELD_TYPES,
    }.get(table_type)
    if field_types is None:
        raise ValueError(f"不支持的表类型: {table_type}")
    return {
        field: TYPE_CONVERTERS.get(field_type, safe_str_convert)
        for field, field_type in field_types.items()
    }
