# 经费收支目录处理说明

## Excel 位置契约

- 第2行 C-H：经费号、项目名称、起始日期、终止日期。
- 第3行 A-H：日期、凭证号、摘要、科目代码、科目名称、借方金额、贷方金额、余额。
- 第4行 C/H：期初余额标签和值。
- 第5行开始：流水明细。
- “累计发生额”行：借方、贷方累计。
- “期末余额”行：期末余额。

程序只读取每个文件的第一个工作表，并将经费号截取为头部前8位数字。

## 保存结构

- `rms_fund_statement`：保存经费号、项目名称、期间和余额汇总。
- `rms_fund_transaction`：保存流水明细。

零流水文件只保存汇总，不创建占位流水。汇总和同一文件的流水在一个 SQLite 事务内提交。

收支流水业务唯一键保持原代码规则：

```text
fund_id + transaction_date + voucher_number + debit_amount + balance
```

## 推送

一个期间汇总作为一条 `FUND_STATEMENT` 记录推送，其 `data.transactions` 包含本次文件清洗后的流水数组。

## 命令行

```powershell
python run_transactions_processor.py --folder "D:\path\to\folder"
python run_transactions_processor.py --folder "D:\path\to\folder" --no-push
```
