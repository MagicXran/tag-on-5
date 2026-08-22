"""收支明细目录解析、SQLite事务保存和第三方推送。"""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from config import (
    TRANSACTIONS_COLUMN_MAPPING,
    TRANSACTIONS_EXCEL_CONSTANTS,
    TYPE_CONVERTERS,
    get_database_config,
)
from database_manager_sqlite import DatabaseManager
from logger_utils import LoggerManager, process_logger
from third_party_client import ThirdPartyClient


_DEFAULT_CLIENT = object()


class TransactionsProcessor:
    def __init__(
        self,
        db_manager: Optional[DatabaseManager] = None,
        third_party_client: Any = _DEFAULT_CLIENT,
    ):
        self.logger = LoggerManager.get_logger("TransactionsProcessor")
        self.db_manager = db_manager or DatabaseManager(get_database_config())
        self.third_party_client = (
            ThirdPartyClient() if third_party_client is _DEFAULT_CLIENT else third_party_client
        )
        self.excel_constants = TRANSACTIONS_EXCEL_CONSTANTS

    def process_transactions_folder(self, folder_path: str, push: bool = True) -> Dict[str, Any]:
        if not folder_path or not os.path.isdir(folder_path):
            raise ValueError(f"收支明细目录不存在: {folder_path}")
        files = self._get_excel_files(folder_path)
        if not files:
            return {
                "success": False,
                "message": "未找到Excel文件",
                "processed_files": 0,
                "failed_files": 0,
                "successful_records": 0,
                "failed_records": 0,
                "errors": [],
                "push_result": None,
            }

        results = {
            "success": True,
            "processed_files": 0,
            "failed_files": 0,
            "total_records": 0,
            "successful_records": 0,
            "failed_records": 0,
            "statement_inserted": 0,
            "statement_updated": 0,
            "transaction_inserted": 0,
            "transaction_updated": 0,
            "errors": [],
            "push_result": None,
        }
        push_records: List[Dict[str, Any]] = []
        for file_path in files:
            try:
                statement, transactions = self.parse_transactions_file(file_path)
                save_result = self.db_manager.save_fund_statement(statement, transactions)
                push_records.append(
                    {
                        "operation": save_result["operation"],
                        "businessKey": save_result["businessKey"],
                        "data": save_result["data"],
                    }
                )
                results["processed_files"] += 1
                results["total_records"] += len(transactions)
                results["successful_records"] += len(transactions)
                results[
                    "statement_inserted" if save_result["operation"] == "INSERT" else "statement_updated"
                ] += 1
                results["transaction_inserted"] += save_result["transaction_inserted"]
                results["transaction_updated"] += save_result["transaction_updated"]
            except Exception as exc:
                results["failed_files"] += 1
                results["errors"].append(f"{file_path}: {exc}")
                self.logger.exception("处理收支文件失败: %s", file_path)

        if push and self.third_party_client and push_records:
            results["push_result"] = self.third_party_client.push_records(
                "FUND_STATEMENT", push_records
            )
            if not results["push_result"]["success"]:
                results["errors"].append("SQLite保存成功、第三方推送失败")
        results["success"] = results["failed_files"] == 0 and not results["errors"]
        process_logger.log_end(
            "收支目录处理",
            文件数=len(files),
            成功文件=results["processed_files"],
            失败文件=results["failed_files"],
            汇总新增=results["statement_inserted"],
            汇总更新=results["statement_updated"],
            流水新增=results["transaction_inserted"],
            流水更新=results["transaction_updated"],
        )
        return results

    def parse_transactions_file(
        self, excel_file_path: str
    ) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        path = Path(excel_file_path)
        if not path.exists():
            raise FileNotFoundError(f"Excel文件不存在: {excel_file_path}")
        with pd.ExcelFile(path) as workbook:
            sheet_names = list(workbook.sheet_names)
            if len(sheet_names) > 1:
                process_logger.log_warning(
                    "收支工作表",
                    "只处理第一个工作表",
                    file_path=excel_file_path,
                    ignored_sheets=sheet_names[1:],
                )
            df = pd.read_excel(workbook, sheet_name=sheet_names[0], header=None)
        fund_info = self._parse_fund_header(df)
        self._validate_field_row(df)
        transactions = self._parse_transactions_data(df, fund_info)
        summary = self._parse_summary_data(df)
        statement = {
            "fund_id": fund_info["fund_id"],
            "project_name": fund_info["project_name"],
            "period_start": fund_info["period_start"],
            "period_end": fund_info["period_end"],
            "opening_balance": self._parse_opening_balance(df, fund_info["fund_id"]),
            "total_debit": summary["total_debit"],
            "total_credit": summary["total_credit"],
            "ending_balance": summary["ending_balance"],
        }
        process_logger.log_excel_operation(
            "解析收支文件",
            excel_file_path,
            fund_id=statement["fund_id"],
            period=f"{statement['period_start']}~{statement['period_end']}",
            流水数=len(transactions),
        )
        return statement, transactions

    def _parse_fund_header(self, df: pd.DataFrame) -> Dict[str, str]:
        row = self.excel_constants["HEADER_ROW"] - 1
        content = ""
        for column in range(
            self.excel_constants["HEADER_COLUMN_START"],
            self.excel_constants["HEADER_COLUMN_END"] + 1,
        ):
            if row < len(df) and column < len(df.columns):
                value = df.iloc[row, column]
                if pd.notna(value) and str(value).strip():
                    content = str(value).strip()
                    break
        if not content:
            raise ValueError("第2行C-H未找到经费信息")
        match = re.match(r"^(\d+)", content)
        if not match or len(match.group(1)) < 8:
            raise ValueError(f"无法解析8位经费号: {content}")
        fund_id = match.group(1)[:8]
        remaining = content[8:].strip()
        remaining = re.sub(r"^[（(][^）)]*[）)]", "", remaining).strip()
        start_match = re.search(r"起始日期[：:]\s*(\d{4}-\d{2}-\d{2})", remaining)
        end_match = re.search(r"终止日期[：:]\s*(\d{4}-\d{2}-\d{2})", remaining)
        if not start_match or not end_match:
            raise ValueError(f"经费信息缺少起止日期: {content}")
        project_name = re.sub(
            r"\s*起始日期[：:]\s*\d{4}-\d{2}-\d{2}\s*", " ", remaining
        )
        project_name = re.sub(
            r"\s*终止日期[：:]\s*\d{4}-\d{2}-\d{2}\s*", " ", project_name
        ).strip()
        return {
            "fund_id": fund_id,
            "project_name": project_name,
            "period_start": start_match.group(1),
            "period_end": end_match.group(1),
        }

    def _validate_field_row(self, df: pd.DataFrame):
        row = self.excel_constants["FIELD_ROW"] - 1
        actual = [
            "" if pd.isna(df.iloc[row, column]) else str(df.iloc[row, column]).strip()
            for column in range(8)
        ]
        expected = self.excel_constants["FIELD_NAMES"]
        if actual != expected:
            raise ValueError(f"收支第3行字段不匹配: expected={expected}, actual={actual}")

    def _parse_transactions_data(
        self, df: pd.DataFrame, fund_info: Dict[str, str]
    ) -> List[Dict[str, Any]]:
        records: List[Dict[str, Any]] = []
        start = self.excel_constants["DATA_START_ROW"] - 1
        for index in range(start, len(df)):
            row = df.iloc[index]
            description = "" if pd.isna(row.iloc[2]) else str(row.iloc[2]).strip()
            if any(keyword in description for keyword in self.excel_constants["SUMMARY_KEYWORDS"]):
                break
            if row.isna().all():
                continue
            chinese_record = {
                "经费卡号": fund_info["fund_id"],
                "序号1": len(records) + 1,
            }
            for column, field_name in enumerate(self.excel_constants["FIELD_NAMES"]):
                chinese_record[field_name] = row.iloc[column]
            record: Dict[str, Any] = {}
            for chinese_name, english_name in TRANSACTIONS_COLUMN_MAPPING.items():
                value = chinese_record.get(chinese_name)
                if chinese_name in {"借方金额", "贷方金额", "余额"}:
                    converter = TYPE_CONVERTERS["float"]
                elif chinese_name == "序号1":
                    converter = TYPE_CONVERTERS["int"]
                elif chinese_name == "日期":
                    converter = TYPE_CONVERTERS["date"]
                else:
                    converter = TYPE_CONVERTERS["str"]
                record[english_name] = converter(value)
            if not record["transaction_date"]:
                raise ValueError(f"第{index + 1}行交易日期为空")
            records.append(record)
        return records

    def _parse_summary_data(self, df: pd.DataFrame) -> Dict[str, float]:
        result = {"total_debit": 0.0, "total_credit": 0.0, "ending_balance": 0.0}
        found_cumulative = False
        found_ending = False
        for index in range(len(df)):
            description = "" if pd.isna(df.iloc[index, 2]) else str(df.iloc[index, 2]).strip()
            if "累计发生额" in description:
                result["total_debit"] = TYPE_CONVERTERS["float"](df.iloc[index, 5])
                result["total_credit"] = TYPE_CONVERTERS["float"](df.iloc[index, 6])
                found_cumulative = True
            elif "期末余额" in description:
                result["ending_balance"] = TYPE_CONVERTERS["float"](df.iloc[index, 7])
                found_ending = True
        if not found_cumulative or not found_ending:
            raise ValueError("未找到唯一的累计发生额或期末余额行")
        return result

    def _parse_opening_balance(self, df: pd.DataFrame, fund_id: str) -> float:
        label = "" if pd.isna(df.iloc[3, 2]) else str(df.iloc[3, 2]).strip()
        if "期初余额" not in label:
            raise ValueError(f"经费号{fund_id}第4行C列不是期初余额")
        return TYPE_CONVERTERS["float"](df.iloc[3, 7])

    @staticmethod
    def _get_excel_files(folder_path: str) -> List[str]:
        files: List[str] = []
        for root, _, names in os.walk(folder_path):
            for name in names:
                if name.lower().endswith((".xls", ".xlsx")):
                    files.append(os.path.join(root, name))
        return sorted(files)


def main():
    from config import EXCEL_FILES

    folder = EXCEL_FILES.get("transactions")
    if not folder:
        raise SystemExit("请通过GUI或配置指定收支目录")
    result = TransactionsProcessor().process_transactions_folder(folder)
    print(result)


if __name__ == "__main__":
    main()
