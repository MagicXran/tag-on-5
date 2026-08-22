"""SQLite v2 数据库访问层。"""

from __future__ import annotations

import os
import sqlite3
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from logger_utils import process_logger


CONTRACT_COLUMNS = [
    "contract_id", "contract_name", "leader", "contract_funds", "contract_category",
    "effective_date", "undertaking_unit", "project_members", "registration_id",
    "start_date", "end_date", "leader_type", "leader_phone", "leader_email",
    "operator", "operator_phone", "validity_status", "progress_status",
    "payment_method", "party_a_sealed", "party_b_sealed", "contract_recovered",
    "statistical_attribution", "primary_discipline", "research_category",
    "cooperation_form", "project_source", "socioeconomic_target",
    "national_economy_industry", "audit_status", "remarks", "is_effective",
    "party_a_name", "party_a_contact", "party_a_phone", "party_a_type",
    "party_a_province", "party_a_city", "party_a_postal_code", "party_a_address",
    "patent_count", "amount_received", "intellectual_property_number",
    "leader_employee_id", "software_copyright_count", "fund_id", "conversion_type",
    "license_type", "purchase_funds", "cooperation_funds",
]

FUND_RECEIPT_COLUMNS = [
    "unid", "project_name", "fund_manager", "funds_received", "fund_id",
    "allocation_date", "project_unit", "contract_id", "approval_number",
    "approved_funds", "manager_employee_id", "project_leader", "fund_unit",
    "project_category", "receipt_number", "retained_funds", "allocated_funds",
    "payment_unit", "payment_type", "audit_status", "project_nature", "project_level",
]

TRANSACTION_COLUMNS = [
    "fund_id", "sequence_number", "transaction_date", "voucher_number", "summary",
    "subject_code", "subject_name", "debit_amount", "credit_amount", "balance",
]


class DatabaseManager:
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or {"database": "data/rms_v2.db"}
        self.db_path = self.config.get("database", "data/rms_v2.db")
        self.connection: Optional[sqlite3.Connection] = None
        db_dir = os.path.dirname(os.path.abspath(self.db_path))
        if db_dir:
            os.makedirs(db_dir, exist_ok=True)
        self._init_database()

    def get_connection(self) -> sqlite3.Connection:
        if self.connection is None:
            # GUI在主线程创建管理器，在后台工作线程执行导入；连接允许跨线程串行使用。
            self.connection = sqlite3.connect(self.db_path, check_same_thread=False)
            self.connection.row_factory = sqlite3.Row
            self.connection.execute("PRAGMA foreign_keys=ON")
            self.connection.execute("PRAGMA journal_mode=WAL")
        return self.connection

    def close_connection(self):
        if self.connection is not None:
            self.connection.close()
            self.connection = None

    def refresh_connection(self):
        self.close_connection()
        self.get_connection()

    def validate_config(self) -> Tuple[bool, str]:
        return (bool(self.db_path), "SQLite配置有效" if self.db_path else "数据库路径为空")

    def test_connection(self) -> Tuple[bool, str]:
        try:
            row = self.get_connection().execute("SELECT 1").fetchone()
            return (row[0] == 1, f"SQLite连接成功: {self.db_path}")
        except Exception as exc:
            return False, str(exc)

    def get_connection_diagnostic_info(self) -> Dict[str, Any]:
        valid, config_message = self.validate_config()
        connected, connection_message = self.test_connection()
        return {
            "config_valid": valid,
            "config_message": config_message,
            "connection_test": connected,
            "connection_message": connection_message,
            "detailed_error": "" if connected else connection_message,
        }

    def _init_database(self):
        conn = self.get_connection()
        with conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS rms_contract (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    contract_id TEXT NOT NULL,
                    contract_name TEXT,
                    leader TEXT,
                    contract_funds REAL,
                    contract_category TEXT,
                    effective_date TEXT,
                    undertaking_unit TEXT,
                    project_members TEXT,
                    registration_id TEXT,
                    start_date TEXT,
                    end_date TEXT,
                    leader_type TEXT,
                    leader_phone TEXT,
                    leader_email TEXT,
                    operator TEXT,
                    operator_phone TEXT,
                    validity_status TEXT,
                    progress_status TEXT,
                    payment_method TEXT,
                    party_a_sealed TEXT,
                    party_b_sealed TEXT,
                    contract_recovered TEXT,
                    statistical_attribution TEXT,
                    primary_discipline TEXT,
                    research_category TEXT,
                    cooperation_form TEXT,
                    project_source TEXT,
                    socioeconomic_target TEXT,
                    national_economy_industry TEXT,
                    audit_status TEXT,
                    remarks TEXT,
                    is_effective TEXT,
                    party_a_name TEXT,
                    party_a_contact TEXT,
                    party_a_phone TEXT,
                    party_a_type TEXT,
                    party_a_province TEXT,
                    party_a_city TEXT,
                    party_a_postal_code TEXT,
                    party_a_address TEXT,
                    patent_count INTEGER,
                    amount_received REAL,
                    intellectual_property_number TEXT,
                    leader_employee_id TEXT,
                    software_copyright_count INTEGER,
                    fund_id TEXT NOT NULL DEFAULT '',
                    conversion_type TEXT,
                    license_type TEXT,
                    purchase_funds REAL,
                    cooperation_funds REAL,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(contract_id, fund_id)
                );

                CREATE TABLE IF NOT EXISTS rms_fund_receipt (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    unid TEXT NOT NULL UNIQUE,
                    project_name TEXT,
                    fund_manager TEXT,
                    funds_received REAL,
                    fund_id TEXT,
                    allocation_date TEXT,
                    project_unit TEXT,
                    contract_id TEXT,
                    approval_number TEXT,
                    approved_funds REAL,
                    manager_employee_id TEXT,
                    project_leader TEXT,
                    fund_unit TEXT,
                    project_category TEXT,
                    receipt_number TEXT,
                    retained_funds REAL,
                    allocated_funds REAL,
                    payment_unit TEXT,
                    payment_type TEXT,
                    audit_status TEXT,
                    project_nature TEXT,
                    project_level TEXT,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS rms_fund_statement (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    fund_id TEXT NOT NULL,
                    project_name TEXT,
                    period_start TEXT NOT NULL,
                    period_end TEXT NOT NULL,
                    opening_balance REAL,
                    total_debit REAL,
                    total_credit REAL,
                    ending_balance REAL,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(fund_id, period_start, period_end)
                );

                CREATE TABLE IF NOT EXISTS rms_fund_transaction (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    fund_id TEXT NOT NULL,
                    sequence_number INTEGER,
                    transaction_date TEXT NOT NULL,
                    voucher_number TEXT NOT NULL,
                    summary TEXT,
                    subject_code TEXT,
                    subject_name TEXT,
                    debit_amount REAL NOT NULL DEFAULT 0,
                    credit_amount REAL,
                    balance REAL NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(fund_id, transaction_date, voucher_number, debit_amount, balance)
                );
                """
            )
        process_logger.logger.info("SQLite v2数据库初始化完成: %s", self.db_path)

    def execute_query(
        self,
        sql: str,
        params: Optional[tuple] = None,
        fetch_one: bool = False,
        fetch_all: bool = True,
    ):
        cursor = self.get_connection().execute(sql, params or ())
        if fetch_one:
            row = cursor.fetchone()
            return dict(row) if row else None
        if fetch_all:
            return [dict(row) for row in cursor.fetchall()]
        return cursor.rowcount

    def execute_update(self, sql: str, params: Optional[tuple] = None) -> int:
        conn = self.get_connection()
        try:
            cursor = conn.execute(sql, params or ())
            conn.commit()
            return cursor.rowcount
        except Exception:
            conn.rollback()
            raise

    def save_contracts_data(self, df: pd.DataFrame) -> Tuple[List[Dict], List[Dict]]:
        inserted: List[Dict] = []
        updated: List[Dict] = []
        conn = self.get_connection()
        try:
            for _, row in df.iterrows():
                data = self._clean_record(row.to_dict(), CONTRACT_COLUMNS)
                data["fund_id"] = data.get("fund_id") or ""
                key = (data["contract_id"], data["fund_id"])
                existing = conn.execute(
                    "SELECT id FROM rms_contract WHERE contract_id=? AND fund_id=?", key
                ).fetchone()
                if existing:
                    self._update_record(conn, "rms_contract", data, "id", existing["id"])
                    target = updated
                    operation = "UPDATE"
                else:
                    self._insert_record(conn, "rms_contract", data)
                    target = inserted
                    operation = "INSERT"
                target.append(self._contract_push_record(operation, data))
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        process_logger.log_database_operation(
            "合同upsert", "rms_contract", inserted=len(inserted), updated=len(updated)
        )
        return inserted, updated

    def save_project_funds_data(self, df: pd.DataFrame) -> Tuple[List[Dict], List[Dict]]:
        inserted: List[Dict] = []
        conn = self.get_connection()
        try:
            for _, row in df.iterrows():
                data = self._clean_record(row.to_dict(), FUND_RECEIPT_COLUMNS)
                self._insert_record(conn, "rms_fund_receipt", data)
                inserted.append(
                    {
                        "operation": "INSERT",
                        "businessKey": {"unid": data["unid"]},
                        "data": data,
                    }
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        process_logger.log_database_operation(
            "到账新增", "rms_fund_receipt", inserted=len(inserted), updated=0
        )
        return inserted, []

    def save_fund_statement(
        self, statement: Dict[str, Any], transactions: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        conn = self.get_connection()
        statement_columns = [
            "fund_id", "project_name", "period_start", "period_end", "opening_balance",
            "total_debit", "total_credit", "ending_balance",
        ]
        statement_data = self._clean_record(statement, statement_columns)
        cleaned_transactions = [
            self._clean_record(transaction, TRANSACTION_COLUMNS)
            for transaction in transactions
        ]
        self._validate_statement_transactions(statement_data, cleaned_transactions)
        key = (
            statement_data["fund_id"],
            statement_data["period_start"],
            statement_data["period_end"],
        )
        try:
            existing = conn.execute(
                """SELECT id FROM rms_fund_statement
                   WHERE fund_id=? AND period_start=? AND period_end=?""",
                key,
            ).fetchone()
            if existing:
                self._update_record(conn, "rms_fund_statement", statement_data, "id", existing["id"])
                operation = "UPDATE"
            else:
                self._insert_record(conn, "rms_fund_statement", statement_data)
                operation = "INSERT"

            inserted_count = 0
            updated_count = 0
            saved_transactions: List[Dict[str, Any]] = []
            for data in cleaned_transactions:
                transaction_key = (
                    data["fund_id"], data["transaction_date"], data["voucher_number"],
                    data["debit_amount"], data["balance"],
                )
                existing_transaction = conn.execute(
                    """SELECT id FROM rms_fund_transaction
                       WHERE fund_id=? AND transaction_date=? AND voucher_number=?
                         AND debit_amount=? AND balance=?""",
                    transaction_key,
                ).fetchone()
                if existing_transaction:
                    self._update_record(
                        conn, "rms_fund_transaction", data, "id", existing_transaction["id"]
                    )
                    updated_count += 1
                else:
                    self._insert_record(conn, "rms_fund_transaction", data)
                    inserted_count += 1
                saved_transactions.append(data)
            conn.commit()
        except Exception:
            conn.rollback()
            process_logger.log_error(
                "收支事务保存",
                "汇总或流水保存失败，已回滚",
                fund_id=statement.get("fund_id"),
                period_start=statement.get("period_start"),
                period_end=statement.get("period_end"),
            )
            raise

        push_data = dict(statement_data)
        push_data["transactions"] = saved_transactions
        process_logger.log_database_operation(
            "收支期间保存",
            "rms_fund_statement/rms_fund_transaction",
            statement_operation=operation,
            transaction_inserted=inserted_count,
            transaction_updated=updated_count,
        )
        return {
            "operation": operation,
            "businessKey": {
                "fundId": statement_data["fund_id"],
                "periodStart": statement_data["period_start"],
                "periodEnd": statement_data["period_end"],
            },
            "data": push_data,
            "transaction_inserted": inserted_count,
            "transaction_updated": updated_count,
        }

    @classmethod
    def _validate_statement_transactions(
        cls,
        statement: Dict[str, Any],
        transactions: List[Dict[str, Any]],
    ):
        period_start = cls._parse_iso_date(statement["period_start"], "period_start")
        period_end = cls._parse_iso_date(statement["period_end"], "period_end")
        if period_start > period_end:
            raise ValueError("汇总期间起始日期晚于终止日期")

        for transaction in transactions:
            if transaction["fund_id"] != statement["fund_id"]:
                raise ValueError(
                    "流水经费号不一致: "
                    f"statement={statement['fund_id']}, transaction={transaction['fund_id']}"
                )
            transaction_date = cls._parse_iso_date(
                transaction["transaction_date"], "transaction_date"
            )
            if not period_start <= transaction_date <= period_end:
                raise ValueError(
                    "流水日期不在汇总期间: "
                    f"date={transaction['transaction_date']}, "
                    f"period={statement['period_start']}~{statement['period_end']}"
                )

    @staticmethod
    def _parse_iso_date(value: Any, field_name: str):
        text = str(value).strip()
        try:
            parsed = datetime.strptime(text, "%Y-%m-%d").date()
        except ValueError as exc:
            raise ValueError(f"{field_name}不是有效的YYYY-MM-DD日期: {value}") from exc
        if parsed.strftime("%Y-%m-%d") != text:
            raise ValueError(f"{field_name}不是有效的YYYY-MM-DD日期: {value}")
        return parsed

    def _insert_record(self, conn: sqlite3.Connection, table: str, data: Dict[str, Any]) -> int:
        columns = list(data)
        sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({', '.join('?' for _ in columns)})"
        cursor = conn.execute(sql, tuple(data[column] for column in columns))
        return int(cursor.lastrowid)

    def _update_record(
        self, conn: sqlite3.Connection, table: str, data: Dict[str, Any], key_column: str, key: Any
    ):
        columns = list(data)
        assignments = ", ".join(f"{column}=?" for column in columns)
        sql = f"UPDATE {table} SET {assignments}, updated_at=CURRENT_TIMESTAMP WHERE {key_column}=?"
        conn.execute(sql, tuple(data[column] for column in columns) + (key,))

    @staticmethod
    def _clean_record(data: Dict[str, Any], allowed_columns: List[str]) -> Dict[str, Any]:
        result: Dict[str, Any] = {}
        for column in allowed_columns:
            value = data.get(column)
            if value is None or (not isinstance(value, (list, dict)) and pd.isna(value)):
                value = "" if column not in {
                    "contract_funds", "amount_received", "purchase_funds", "cooperation_funds",
                    "funds_received", "approved_funds", "retained_funds", "allocated_funds",
                    "opening_balance", "total_debit", "total_credit", "ending_balance",
                    "debit_amount", "credit_amount", "balance", "patent_count",
                    "software_copyright_count", "sequence_number",
                } else 0
            result[column] = value
        return result

    @staticmethod
    def _contract_push_record(operation: str, data: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "operation": operation,
            "businessKey": {
                "contractId": data["contract_id"],
                "fundId": data["fund_id"],
            },
            "data": data,
        }

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close_connection()
