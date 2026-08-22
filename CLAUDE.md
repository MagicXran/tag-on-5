# RMS Repository Guide

## System

This Windows Python application imports three Excel data sets: contracts, fund receipts, and fund statements. It validates the latest source formats, cleans the data, writes `data/rms_v2.db`, then actively pushes batches to a third-party HTTP endpoint.

There is no active OA or MySQL integration.

## Commands

```powershell
python run_gui.py
python -m unittest discover -s tests -v
python run_processor.py --check
python run_transactions_processor.py --folder "D:\path" --no-push
```

## Core invariants

- Never modify or migrate `data/rms.db`.
- Contract business key: `contract_id + fund_id`.
- Fund receipt business key: generated `unid`; every import is a new receipt.
- Transaction business key: `fund_id + transaction_date + voucher_number + debit_amount + balance`.
- Read only the first worksheet.
- A fund statement and its transactions save in one SQLite transaction.
- Zero-transaction statements do not create placeholder transactions.
- Push retry uses the same `requestId`, waits three seconds, and makes at most four total requests.
