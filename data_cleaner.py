"""合同和经费到账 Excel 的校验、清洗与字段映射。"""

from __future__ import annotations

import uuid
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

from config import (
    CONTRACTS_COLUMN_MAPPING,
    CONTRACTS_EXPECTED_COLUMNS,
    CONTRACTS_FIELD_TYPES,
    PROJECT_FUNDS_COLUMN_MAPPING,
    PROJECT_FUNDS_EXPECTED_COLUMNS,
    PROJECT_FUNDS_FIELD_TYPES,
    TYPE_CONVERTERS,
    get_excel_converters,
)
from logger_utils import process_logger


class DataCleaner:
    """按最新版源表契约输出可直接落库的数据。"""

    def clean_fund_ids(self, value: Any) -> List[str]:
        if value is None or pd.isna(value) or str(value).strip() == "":
            return []
        result: List[str] = []
        for item in str(value).split(","):
            cleaned = item.strip()
            if cleaned.endswith(".0") and cleaned[:-2].isdigit():
                cleaned = cleaned[:-2]
            if cleaned and cleaned.lower() not in {"nan", "none", "null"} and cleaned not in result:
                result.append(cleaned)
        return result

    def clean_fund_ids_to_string(self, value: Any) -> str:
        return ",".join(self.clean_fund_ids(value))

    def clean_dataframe(self, df: pd.DataFrame, field_types: Dict[str, str]) -> pd.DataFrame:
        cleaned = df.copy()
        for column, field_type in field_types.items():
            converter = TYPE_CONVERTERS[field_type]
            cleaned[column] = cleaned[column].apply(converter)
        return cleaned

    def map_columns(self, df: pd.DataFrame, mapping: Dict[str, str]) -> pd.DataFrame:
        return df.rename(columns=mapping)[list(mapping.values())].copy()

    def split_fundids_rows(self, df: pd.DataFrame) -> pd.DataFrame:
        rows: List[Dict[str, Any]] = []
        multi_count = 0
        for _, row in df.iterrows():
            fund_ids = self.clean_fund_ids(row.get("fund_id", ""))
            if len(fund_ids) > 1:
                multi_count += 1
            for fund_id in fund_ids or [""]:
                item = row.to_dict()
                item["fund_id"] = fund_id
                rows.append(item)
        result = pd.DataFrame(rows, columns=df.columns)
        process_logger.log_data_stats(
            "经费号拆分",
            "rms_contract",
            原始行数=len(df),
            拆分后行数=len(result),
            多经费号行数=multi_count,
        )
        return result

    def process_contracts_excel(self, file_path: str) -> pd.DataFrame:
        process_logger.log_start("处理合同清单", file_path=file_path)
        df = self._read_first_sheet(
            file_path,
            CONTRACTS_EXPECTED_COLUMNS,
            get_excel_converters("contracts"),
        )
        original_count = len(df)
        cleaned = self.clean_dataframe(df, CONTRACTS_FIELD_TYPES)
        mapped = self.map_columns(cleaned, CONTRACTS_COLUMN_MAPPING)
        filtered = mapped[mapped["contract_id"].astype(str).str.strip().ne("")].copy()
        process_logger.log_filter_result(
            "合同编号非空",
            original_count,
            len(filtered),
            "contract_id 非空",
        )
        result = self.split_fundids_rows(filtered)
        process_logger.log_end("处理合同清单", 最终记录数=len(result))
        return result

    def process_project_funds_excel(self, file_path: str) -> pd.DataFrame:
        process_logger.log_start("处理经费到账清单", file_path=file_path)
        df = self._read_first_sheet(
            file_path,
            PROJECT_FUNDS_EXPECTED_COLUMNS,
            get_excel_converters("project_funds"),
        )
        original_count = len(df)
        cleaned = self.clean_dataframe(df, PROJECT_FUNDS_FIELD_TYPES)
        mapped = self.map_columns(cleaned, PROJECT_FUNDS_COLUMN_MAPPING)
        condition = (
            mapped["fund_id"].astype(str).str.strip().ne("")
            & mapped["allocation_date"].astype(str).str.strip().ne("")
            & mapped["contract_id"].astype(str).str.strip().ne("")
        )
        filtered = mapped[condition].copy()
        filtered["unid"] = [str(uuid.uuid4()) for _ in range(len(filtered))]
        process_logger.log_filter_result(
            "到账必填字段",
            original_count,
            len(filtered),
            "经费账号、拨款时间、项目编号非空",
        )
        process_logger.log_end("处理经费到账清单", 最终记录数=len(filtered))
        return filtered

    def _read_first_sheet(
        self,
        file_path: str,
        expected_columns: List[str],
        converters: Dict[str, Any],
    ) -> pd.DataFrame:
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"Excel 文件不存在: {file_path}")
        with pd.ExcelFile(path) as workbook:
            sheet_names = list(workbook.sheet_names)
            if len(sheet_names) > 1:
                process_logger.log_warning(
                    "Excel工作表",
                    "只处理第一个工作表",
                    file_path=file_path,
                    ignored_sheets=sheet_names[1:],
                )
            df = pd.read_excel(workbook, sheet_name=sheet_names[0], converters=converters)
        actual_columns = [str(column).strip() for column in df.columns]
        if actual_columns != expected_columns:
            missing = [column for column in expected_columns if column not in actual_columns]
            extra = [column for column in actual_columns if column not in expected_columns]
            raise ValueError(
                f"Excel表头不符合最新版格式: missing={missing}, extra={extra}, "
                f"expected_count={len(expected_columns)}, actual_count={len(actual_columns)}"
            )
        process_logger.log_excel_operation(
            "读取第一个工作表",
            file_path,
            sheet=sheet_names[0],
            行数=len(df),
            列数=len(df.columns),
        )
        return df
