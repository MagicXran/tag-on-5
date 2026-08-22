"""第三方统一批量推送客户端。"""

from __future__ import annotations

import time
import uuid
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional

import requests

from config import THIRD_PARTY_CONFIG
from logger_utils import process_logger


class ThirdPartyClient:
    def __init__(
        self,
        config: Optional[Dict[str, Any]] = None,
        post: Optional[Callable[..., Any]] = None,
        sleep: Optional[Callable[[float], None]] = None,
    ):
        self.config = dict(THIRD_PARTY_CONFIG)
        if config:
            self.config.update(config)
        self.post = post or requests.post
        self.sleep = sleep or time.sleep

    @property
    def url(self) -> str:
        return (
            self.config["base_url"].rstrip("/")
            + "/"
            + self.config.get("endpoint", "/api/v1/rms/batch-import").lstrip("/")
        )

    def push_records(self, data_type: str, records: List[Dict[str, Any]]) -> Dict[str, Any]:
        if not records:
            return {"success": True, "attempts": 0, "batches": [], "failed_batches": 0}
        batch_size = int(self.config.get("batch_size", 100))
        batch_results = []
        for start in range(0, len(records), batch_size):
            batch = records[start : start + batch_size]
            batch_results.append(self._push_batch(data_type, batch, start // batch_size + 1))
        return {
            "success": all(item["success"] for item in batch_results),
            "attempts": sum(item["attempts"] for item in batch_results),
            "batches": batch_results,
            "failed_batches": sum(not item["success"] for item in batch_results),
        }

    def _push_batch(
        self, data_type: str, records: List[Dict[str, Any]], batch_number: int
    ) -> Dict[str, Any]:
        request_id = str(uuid.uuid4())
        payload = {
            "requestId": request_id,
            "dataType": data_type,
            "sentAt": datetime.now().astimezone().isoformat(timespec="seconds"),
            "records": records,
        }
        max_attempts = int(self.config.get("max_retries", 3)) + 1
        last_error = ""
        for attempt in range(1, max_attempts + 1):
            process_logger.log_sync_operation(
                "发送批次",
                data_type=data_type,
                request_id=request_id,
                batch=batch_number,
                record_count=len(records),
                attempt=attempt,
                max_attempts=max_attempts,
            )
            try:
                response = self.post(
                    self.url,
                    json=payload,
                    timeout=self.config.get("timeout", 60),
                    headers={"Content-Type": "application/json; charset=utf-8"},
                )
                if response.status_code != 200:
                    raise RuntimeError(f"HTTP {response.status_code}")
                body = response.json()
                self._validate_response(body, len(records))
                process_logger.log_sync_operation(
                    "批次成功",
                    data_type=data_type,
                    request_id=request_id,
                    batch=batch_number,
                    attempt=attempt,
                    success_count=body.get("successCount"),
                )
                return {
                    "success": True,
                    "request_id": request_id,
                    "attempts": attempt,
                    "record_count": len(records),
                    "response": body,
                }
            except Exception as exc:
                last_error = f"{type(exc).__name__}: {exc}"
                process_logger.log_error(
                    "第三方推送",
                    last_error,
                    data_type=data_type,
                    request_id=request_id,
                    batch=batch_number,
                    attempt=attempt,
                )
                if attempt < max_attempts:
                    self.sleep(float(self.config.get("retry_interval", 3)))
        failed_business_keys = [record.get("businessKey", {}) for record in records]
        process_logger.log_error(
            "第三方推送最终失败",
            last_error,
            data_type=data_type,
            request_id=request_id,
            batch=batch_number,
            failed_business_keys=failed_business_keys,
        )
        return {
            "success": False,
            "request_id": request_id,
            "attempts": max_attempts,
            "record_count": len(records),
            "error": last_error,
            "failed_business_keys": failed_business_keys,
        }

    @staticmethod
    def _validate_response(body: Any, expected_count: int):
        if not isinstance(body, dict):
            raise ValueError("响应JSON必须是对象")
        code = ThirdPartyClient._require_integer(body, "code")
        failed_count = ThirdPartyClient._require_integer(body, "failedCount")
        success_count = ThirdPartyClient._require_integer(body, "successCount")
        if code != 0:
            raise ValueError(f"业务返回码失败: {code} {body.get('message', '')}")
        if failed_count != 0:
            raise ValueError(f"存在失败记录: failedCount={failed_count}")
        if success_count != expected_count:
            raise ValueError(
                f"成功数量不匹配: expected={expected_count}, actual={success_count}"
            )

    @staticmethod
    def _require_integer(body: Dict[str, Any], field_name: str) -> int:
        value = body.get(field_name)
        if type(value) is not int:
            raise ValueError(f"响应字段{field_name}必须是整数: {value!r}")
        return value
