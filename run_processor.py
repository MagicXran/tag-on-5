#!/usr/bin/env python3
"""合同和经费到账命令行入口；日常使用优先运行 run_gui.py。"""

import argparse
import sys

from main_processor import RMSTagOnProcessor


def parse_args():
    parser = argparse.ArgumentParser(description="RMS合同/到账导入处理器")
    parser.add_argument("--contracts-folder")
    parser.add_argument("--funds-folder")
    parser.add_argument("--no-push", action="store_true")
    parser.add_argument("--check", action="store_true")
    return parser.parse_args()


def main():
    args = parse_args()
    processor = RMSTagOnProcessor()
    try:
        if args.check:
            success, message = processor.database_manager.test_connection()
            print(message)
            return 0 if success else 1
        if not args.contracts_folder and not args.funds_folder:
            print("请指定目录参数，或运行 python run_gui.py 使用图形界面。")
            return 1
        has_error = False
        if args.contracts_folder:
            result = processor.process_contracts_folder(
                args.contracts_folder, push=not args.no_push
            )
            print("合同:", result)
            has_error = has_error or not result.get("success", False)
        if args.funds_folder:
            result = processor.process_project_funds_folder(
                args.funds_folder, push=not args.no_push
            )
            print("经费到账:", result)
            has_error = has_error or not result.get("success", False)
        return 1 if has_error else 0
    finally:
        processor.close()


if __name__ == "__main__":
    sys.exit(main())
