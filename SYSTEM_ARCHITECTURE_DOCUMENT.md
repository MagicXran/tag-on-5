# RMS 数据导入系统架构

## 主流程

```text
Tkinter GUI
  → 合同/到账 DataCleaner
  → 收支 TransactionsProcessor
  → DatabaseManager (data/rms_v2.db)
  → ThirdPartyClient
  → POST /api/v1/rms/batch-import
```

## 模块职责

- `config.py`：最新版源表字段契约、SQLite和第三方配置。
- `data_cleaner.py`：合同50字段、到账21字段校验、清洗、筛选和合同经费号拆分。
- `transactions_processor.py`：收支固定位置解析、期间汇总、流水清洗和目录编排。
- `database_manager_sqlite.py`：SQLite v2四表、原业务唯一键和事务写入。
- `third_party_client.py`：统一批量请求、响应严格校验和固定间隔重试。
- `main_processor.py`：合同与到账目录处理编排。
- `gui_main.py`：目录选择、处理开关、后台执行和结果展示。

## 数据表

| 表 | 说明 | 唯一键 |
|---|---|---|
| `rms_contract` | 拆分后的合同 | `contract_id, fund_id` |
| `rms_fund_receipt` | 经费到账 | `unid` |
| `rms_fund_statement` | 经费期间汇总 | `fund_id, period_start, period_end` |
| `rms_fund_transaction` | 经费流水 | `fund_id, transaction_date, voucher_number, debit_amount, balance` |

收支汇总和同一文件的全部流水在一个SQLite事务内保存。零流水文件只保存汇总。

## 失败边界

- 单个Excel失败：记录异常，继续处理其他文件。
- SQLite事务失败：回滚当前收支文件。
- 第三方推送失败：每隔3秒重试，首次后最多重试3次。
- 最终推送失败：SQLite数据保留，GUI和日志明确报告，不写推送状态表。
