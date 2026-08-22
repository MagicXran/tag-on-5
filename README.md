# RMS Excel 数据导入与第三方推送系统

## 用途

人工选择合同、经费到账、经费收支三个 Excel 目录，程序完成格式校验、提取、筛选和清洗，将结果保存到 `data/rms_v2.db`，随后通过统一批量接口主动推送第三方系统。

原 `data/rms.db` 不参与新流程，也不会被迁移或修改。

## 数据流程

```text
GUI选择目录
  → 读取每个文件的第一个工作表
  → 校验最新版表头/收支固定位置
  → 清洗和业务筛选
  → SQLite事务保存
  → POST /api/v1/rms/batch-import
  → 失败间隔3秒重试，首次后最多重试3次
```

## SQLite v2

| 表 | 业务唯一键 |
|---|---|
| `rms_contract` | `contract_id + fund_id` |
| `rms_fund_receipt` | `unid` |
| `rms_fund_statement` | `fund_id + period_start + period_end` |
| `rms_fund_transaction` | `fund_id + transaction_date + voucher_number + debit_amount + balance` |

经费到账沿用原业务规则：每次有效导入生成新的 `unid`，因此每次均新增。

## 启动

```powershell
python -m pip install -r requirements.txt
python run_gui.py
```

第三方地址通过环境变量配置：

```powershell
$env:RMS_THIRD_PARTY_URL = "http://HOST:PORT"
python run_gui.py
```

也可在 `config.py` 的 `THIRD_PARTY_CONFIG` 中调整批大小、超时和重试参数。

## 接口

```http
POST /api/v1/rms/batch-import
Content-Type: application/json
```

```json
{
  "requestId": "UUID",
  "dataType": "CONTRACT",
  "sentAt": "2026-08-22T10:30:00+08:00",
  "records": [
    {
      "operation": "INSERT",
      "businessKey": {},
      "data": {}
    }
  ]
}
```

`dataType` 支持 `CONTRACT`、`FUND_RECEIPT`、`FUND_STATEMENT`。同一批重试始终使用同一个 `requestId`，第三方应以 `requestId + businessKey` 保证幂等。

成功响应必须满足 HTTP 200、`code == 0`、`failedCount == 0`，并且 `successCount` 等于请求记录数。

## 验证

```powershell
python -m unittest discover -s tests -v
python run_processor.py --check
```

日志写入 `logs/rms_tag_on_YYYYMMDD.log`，包括文件处理数量、SQLite新增/更新数量、请求批次、重试次数和错误上下文，不记录完整推送载荷。
