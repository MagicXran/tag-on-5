"""合同、经费到账目录处理和第三方推送编排。"""

from __future__ import annotations

import os
from typing import Any, Dict, List, Optional

from config import get_database_config
from data_cleaner import DataCleaner
from database_manager_sqlite import DatabaseManager
from logger_utils import process_logger
from third_party_client import ThirdPartyClient


_DEFAULT_CLIENT = object()


class RMSTagOnProcessor:
    def __init__(
        self,
        db_manager: Optional[DatabaseManager] = None,
        third_party_client: Any = _DEFAULT_CLIENT,
    ):
        self.data_cleaner = DataCleaner()
        self.database_manager = db_manager or DatabaseManager(get_database_config())
        self.third_party_client = (
            ThirdPartyClient() if third_party_client is _DEFAULT_CLIENT else third_party_client
        )

    def process_contracts_excel(self, file_path: str):
        data = self.data_cleaner.process_contracts_excel(file_path)
        return self.database_manager.save_contracts_data(data)

    def process_project_funds_excel(self, file_path: str):
        data = self.data_cleaner.process_project_funds_excel(file_path)
        return self.database_manager.save_project_funds_data(data)

    def process_contracts_folder(self, folder_path: str, push: bool = False) -> Dict[str, Any]:
        return self._process_folder(
            folder_path,
            self.process_contracts_excel,
            "CONTRACT",
            "合同",
            push,
        )

    def process_project_funds_folder(
        self, folder_path: str, push: bool = False
    ) -> Dict[str, Any]:
        return self._process_folder(
            folder_path,
            self.process_project_funds_excel,
            "FUND_RECEIPT",
            "经费到账",
            push,
        )

    def _process_folder(
        self,
        folder_path: str,
        processor,
        data_type: str,
        label: str,
        push: bool,
    ) -> Dict[str, Any]:
        if not folder_path or not os.path.isdir(folder_path):
            raise ValueError(f"{label}目录不存在: {folder_path}")
        files = self._get_excel_files(folder_path)
        if not files:
            return {
                "success": False,
                "message": "未找到Excel文件",
                "processed_files": 0,
                "total_inserted": 0,
                "total_updated": 0,
                "errors": [],
                "all_inserted_records": [],
                "all_updated_records": [],
                "push_result": None,
            }
        result = {
            "success": True,
            "processed_files": 0,
            "total_inserted": 0,
            "total_updated": 0,
            "errors": [],
            "all_inserted_records": [],
            "all_updated_records": [],
            "push_result": None,
        }
        for file_path in files:
            try:
                inserted, updated = processor(file_path)
                result["processed_files"] += 1
                result["total_inserted"] += len(inserted)
                result["total_updated"] += len(updated)
                result["all_inserted_records"].extend(inserted)
                result["all_updated_records"].extend(updated)
            except Exception as exc:
                result["errors"].append(f"{file_path}: {exc}")
                process_logger.logger.exception("处理%s文件失败: %s", label, file_path)

        records = result["all_inserted_records"] + result["all_updated_records"]
        if push and self.third_party_client and records:
            result["push_result"] = self.third_party_client.push_records(data_type, records)
            if not result["push_result"]["success"]:
                result["errors"].append("SQLite保存成功、第三方推送失败")
        result["success"] = not result["errors"]
        process_logger.log_end(
            f"{label}目录处理",
            处理文件数=result["processed_files"],
            新增=result["total_inserted"],
            更新=result["total_updated"],
            错误数=len(result["errors"]),
        )
        return result

    @staticmethod
    def _get_excel_files(folder_path: str) -> List[str]:
        files: List[str] = []
        for root, _, names in os.walk(folder_path):
            for name in names:
                if name.lower().endswith((".xls", ".xlsx")):
                    files.append(os.path.join(root, name))
        return sorted(files)

    def close(self):
        self.database_manager.close_connection()


class ProcessSummaryReporter:
    @staticmethod
    def generate_summary_report(results: Dict[str, Dict]) -> str:
        lines = ["=" * 60, "RMS数据处理结果", "=" * 60]
        for key, label in (("contracts", "合同"), ("project_funds", "经费到账")):
            item = results.get(key, {})
            lines.append(
                f"{label}: 新增{item.get('total_inserted', 0)}，"
                f"更新{item.get('total_updated', 0)}，"
                f"推送{'成功' if item.get('push_result', {}).get('success') else '失败或跳过'}"
            )
        errors = results.get("errors", [])
        if errors:
            lines.extend(["错误:", *[f"- {error}" for error in errors]])
        return "\n".join(lines)
