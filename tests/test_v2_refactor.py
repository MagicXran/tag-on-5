import sqlite3
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[1]
DATA_ROOT = REPO_ROOT.parent / "最新数据" / "数据测试" / "2026.4.2上传2026年1-3月数据"


class V2DataFlowTests(unittest.TestCase):
    def test_latest_contract_file_produces_expected_split_rows_and_fields(self):
        from data_cleaner import DataCleaner

        source = DATA_ROOT / "合同" / "测试合同.xls"
        result = DataCleaner().process_contracts_excel(str(source))

        self.assertEqual(27, len(result))
        self.assertIn("effective_date", result.columns)
        self.assertIn("conversion_type", result.columns)
        self.assertIn("license_type", result.columns)
        self.assertNotIn("signdate", result.columns)
        self.assertEqual(10, int((result["fund_id"] == "").sum()))

    def test_latest_fund_file_reads_only_first_sheet_and_all_21_fields(self):
        from data_cleaner import DataCleaner

        source = DATA_ROOT / "到账" / "测试到账.xls"
        result = DataCleaner().process_project_funds_excel(str(source))

        self.assertEqual(558, len(result))
        self.assertIn("approval_number", result.columns)
        self.assertEqual(290, int(result["approval_number"].fillna("").ne("").sum()))
        self.assertTrue(result["unid"].is_unique)

    def test_v2_schema_preserves_existing_business_keys(self):
        from database_manager_sqlite import DatabaseManager

        with tempfile.TemporaryDirectory() as tmp:
            db = DatabaseManager({"database": str(Path(tmp) / "rms_v2.db")})
            conn = db.get_connection()

            tables = {
                row[0]
                for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }
            self.assertTrue(
                {
                    "rms_contract",
                    "rms_fund_receipt",
                    "rms_fund_statement",
                    "rms_fund_transaction",
                }.issubset(tables)
            )

            self.assertEqual(
                ["contract_id", "fund_id"],
                self._unique_columns(conn, "rms_contract"),
            )
            self.assertEqual(["unid"], self._unique_columns(conn, "rms_fund_receipt"))
            self.assertEqual(
                ["fund_id", "period_start", "period_end"],
                self._unique_columns(conn, "rms_fund_statement"),
            )
            self.assertEqual(
                [
                    "fund_id",
                    "transaction_date",
                    "voucher_number",
                    "debit_amount",
                    "balance",
                ],
                self._unique_columns(conn, "rms_fund_transaction"),
            )
            db.close_connection()

    def test_contract_upsert_distinguishes_fund_id(self):
        from database_manager_sqlite import DatabaseManager

        with tempfile.TemporaryDirectory() as tmp:
            db = DatabaseManager({"database": str(Path(tmp) / "rms_v2.db")})
            initial = pd.DataFrame(
                [
                    {"contract_id": "C1", "fund_id": "F1", "contract_name": "A"},
                    {"contract_id": "C1", "fund_id": "F2", "contract_name": "A"},
                ]
            )
            inserted, updated = db.save_contracts_data(initial)
            self.assertEqual((2, 0), (len(inserted), len(updated)))

            changed = pd.DataFrame(
                [{"contract_id": "C1", "fund_id": "F1", "contract_name": "B"}]
            )
            inserted, updated = db.save_contracts_data(changed)
            self.assertEqual((0, 1), (len(inserted), len(updated)))
            rows = db.execute_query(
                "SELECT fund_id, contract_name FROM rms_contract ORDER BY fund_id"
            )
            self.assertEqual(
                [("F1", "B"), ("F2", "A")],
                [(row["fund_id"], row["contract_name"]) for row in rows],
            )
            db.close_connection()

    def test_database_manager_can_be_used_by_gui_worker_thread(self):
        from database_manager_sqlite import DatabaseManager

        with tempfile.TemporaryDirectory() as tmp:
            db = DatabaseManager({"database": str(Path(tmp) / "rms_v2.db")})
            errors = []

            def worker():
                try:
                    db.save_contracts_data(
                        pd.DataFrame(
                            [{"contract_id": "C1", "fund_id": "F1", "contract_name": "A"}]
                        )
                    )
                except Exception as exc:
                    errors.append(exc)

            thread = threading.Thread(target=worker)
            thread.start()
            thread.join()
            self.assertEqual([], errors)
            db.close_connection()

    def test_transaction_parser_keeps_statement_separate_from_details(self):
        from database_manager_sqlite import DatabaseManager
        from transactions_processor import TransactionsProcessor

        source = DATA_ROOT / "横向" / "39320026.xls"
        with tempfile.TemporaryDirectory() as tmp:
            db = DatabaseManager({"database": str(Path(tmp) / "rms_v2.db")})
            processor = TransactionsProcessor(db_manager=db, third_party_client=None)
            statement, transactions = processor.parse_transactions_file(str(source))

            self.assertEqual("39320026", statement["fund_id"])
            self.assertEqual("2026-01-01", statement["period_start"])
            self.assertEqual("2026-03-31", statement["period_end"])
            self.assertTrue(transactions)
            self.assertNotIn("opening_balance", transactions[0])
            db.close_connection()

    def test_zero_transaction_file_saves_statement_without_placeholder(self):
        from database_manager_sqlite import DatabaseManager
        from transactions_processor import TransactionsProcessor

        source = DATA_ROOT / "横向" / "38320001.xls"
        with tempfile.TemporaryDirectory() as tmp:
            db = DatabaseManager({"database": str(Path(tmp) / "rms_v2.db")})
            processor = TransactionsProcessor(db_manager=db, third_party_client=None)
            statement, transactions = processor.parse_transactions_file(str(source))
            self.assertEqual([], transactions)

            result = db.save_fund_statement(statement, transactions)
            self.assertEqual("INSERT", result["operation"])
            self.assertEqual(
                1, db.execute_query("SELECT COUNT(*) count FROM rms_fund_statement")[0]["count"]
            )
            self.assertEqual(
                0,
                db.execute_query("SELECT COUNT(*) count FROM rms_fund_transaction")[0][
                    "count"
                ],
            )
            db.close_connection()

    def test_latest_transaction_folder_matches_verified_baseline(self):
        from database_manager_sqlite import DatabaseManager
        from transactions_processor import TransactionsProcessor

        source = DATA_ROOT / "横向"
        with tempfile.TemporaryDirectory() as tmp:
            db = DatabaseManager({"database": str(Path(tmp) / "rms_v2.db")})
            processor = TransactionsProcessor(db_manager=db, third_party_client=None)
            result = processor.process_transactions_folder(str(source), push=False)

            self.assertEqual(474, result["processed_files"])
            self.assertEqual(0, result["failed_files"])
            self.assertEqual(688, result["successful_records"])
            self.assertEqual(
                474,
                db.execute_query("SELECT COUNT(*) count FROM rms_fund_statement")[0]["count"],
            )
            self.assertEqual(
                688,
                db.execute_query("SELECT COUNT(*) count FROM rms_fund_transaction")[0][
                    "count"
                ],
            )
            db.close_connection()

    def test_statement_and_transactions_rollback_together(self):
        from database_manager_sqlite import DatabaseManager

        statement = {
            "fund_id": "F1",
            "project_name": "P1",
            "period_start": "2026-01-01",
            "period_end": "2026-03-31",
            "opening_balance": 0,
            "total_debit": 1,
            "total_credit": 0,
            "ending_balance": 1,
        }
        transactions = [
            {
                "fund_id": "F1",
                "sequence_number": 1,
                "transaction_date": "2026-01-02",
                "voucher_number": "V1",
                "summary": "S",
                "subject_code": "C",
                "subject_name": "N",
                "debit_amount": 1,
                "credit_amount": 0,
                "balance": 1,
            }
        ]
        with tempfile.TemporaryDirectory() as tmp:
            db = DatabaseManager({"database": str(Path(tmp) / "rms_v2.db")})
            original_insert = db._insert_record

            def fail_on_transaction(conn, table, data):
                if table == "rms_fund_transaction":
                    raise RuntimeError("forced failure")
                return original_insert(conn, table, data)

            with patch.object(db, "_insert_record", side_effect=fail_on_transaction):
                with self.assertRaises(RuntimeError):
                    db.save_fund_statement(statement, transactions)
            self.assertEqual(
                0, db.execute_query("SELECT COUNT(*) count FROM rms_fund_statement")[0]["count"]
            )
            db.close_connection()

    @staticmethod
    def _unique_columns(conn: sqlite3.Connection, table: str):
        indexes = conn.execute(f"PRAGMA index_list({table})").fetchall()
        for index in indexes:
            if index[2] == 1:
                columns = conn.execute(f"PRAGMA index_info({index[1]})").fetchall()
                return [column[2] for column in columns]
        return []


if __name__ == "__main__":
    unittest.main()
