import shutil
import tempfile
import unittest
import json
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
LATEST = REPO_ROOT.parent / "最新数据" / "数据测试" / "2026.4.2上传2026年1-3月数据"


class CapturingClient:
    def __init__(self):
        self.calls = []

    def push_records(self, data_type, records):
        self.calls.append((data_type, records))
        return {"success": True, "attempts": 1, "batches": [], "failed_batches": 0}


class MainProcessorTests(unittest.TestCase):
    def test_contract_folder_saves_then_pushes_inserted_records(self):
        from database_manager_sqlite import DatabaseManager
        from main_processor import RMSTagOnProcessor

        with tempfile.TemporaryDirectory() as tmp:
            folder = Path(tmp) / "contracts"
            folder.mkdir()
            shutil.copy2(LATEST / "合同" / "测试合同.xls", folder / "contracts.xls")
            db = DatabaseManager({"database": str(Path(tmp) / "rms_v2.db")})
            client = CapturingClient()
            processor = RMSTagOnProcessor(db_manager=db, third_party_client=client)

            result = processor.process_contracts_folder(str(folder), push=True)

            self.assertEqual(27, result["total_inserted"])
            self.assertEqual(0, result["total_updated"])
            self.assertTrue(result["push_result"]["success"])
            self.assertEqual("CONTRACT", client.calls[0][0])
            self.assertEqual(27, len(client.calls[0][1]))
            json.dumps(client.calls[0][1], ensure_ascii=False)
            db.close_connection()

    def test_fund_folder_saves_558_new_records_and_pushes_them(self):
        from database_manager_sqlite import DatabaseManager
        from main_processor import RMSTagOnProcessor

        with tempfile.TemporaryDirectory() as tmp:
            folder = Path(tmp) / "funds"
            folder.mkdir()
            shutil.copy2(LATEST / "到账" / "测试到账.xls", folder / "funds.xls")
            db = DatabaseManager({"database": str(Path(tmp) / "rms_v2.db")})
            client = CapturingClient()
            processor = RMSTagOnProcessor(db_manager=db, third_party_client=client)

            result = processor.process_project_funds_folder(str(folder), push=True)

            self.assertEqual(558, result["total_inserted"])
            self.assertEqual("FUND_RECEIPT", client.calls[0][0])
            self.assertEqual(558, len(client.calls[0][1]))
            json.dumps(client.calls[0][1], ensure_ascii=False)
            db.close_connection()


if __name__ == "__main__":
    unittest.main()
