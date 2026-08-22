# RMS 第三方数据接收接口文档

**文档版本：** V1.0
**接口用途：** 接收 RMS 数据导入程序推送的合同、经费到账和经费收支数据
**传输协议：** 内网 HTTP + JSON
**字符编码：** UTF-8
**认证方式：** 当前版本不认证

---

## 1. 接口概述

RMS 数据导入程序由操作人员通过 GUI 手工启动。程序完成 Excel 提取、筛选、清洗及 SQLite 保存后，主动调用第三方系统接口。

第三方系统只需提供一个统一批量接收接口：

```http
POST /api/v1/rms/batch-import
Content-Type: application/json; charset=utf-8
```

三类数据通过请求字段 `dataType` 区分：

| dataType | 数据内容 | 单批最大记录数 |
|---|---|---:|
| `CONTRACT` | 合同数据 | 100 |
| `FUND_RECEIPT` | 经费到账数据 | 100 |
| `FUND_STATEMENT` | 经费期间汇总及流水明细 | 100个经费期间 |

> 第三方必须按 `requestId + businessKey` 实现幂等。同一批请求因超时或失败重试时，`requestId`、`businessKey` 和请求内容保持不变，不得生成重复数据。

## 2. 请求报文

### 2.1 顶层结构

```json
{
  "requestId": "6e547a55-8b25-41dc-bb24-fca9c71364fa",
  "dataType": "CONTRACT",
  "sentAt": "2026-08-22T10:30:00+08:00",
  "records": []
}
```

| 字段 | 类型 | 必填 | 说明 |
|---|---|---|---|
| `requestId` | string | 是 | 本批请求 UUID。同一批重试保持不变。 |
| `dataType` | string | 是 | `CONTRACT`、`FUND_RECEIPT` 或 `FUND_STATEMENT`。 |
| `sentAt` | string | 是 | ISO 8601 时间，包含时区，例如 `2026-08-22T10:30:00+08:00`。 |
| `records` | array | 是 | 本批业务记录，数量为 1—100。 |

### 2.2 records 通用结构

```json
{
  "operation": "INSERT",
  "businessKey": {},
  "data": {}
}
```

| 字段 | 类型 | 必填 | 说明 |
|---|---|---|---|
| `operation` | string | 是 | `INSERT` 或 `UPDATE`。表示 RMS 本地数据库本次保存结果。 |
| `businessKey` | object | 是 | 数据业务唯一键。第三方应以此执行幂等新增或更新。 |
| `data` | object | 是 | 完整业务数据。字段随 `dataType` 变化。 |

处理原则：

1. `INSERT` 表示 RMS 本地首次出现该业务键。
2. `UPDATE` 表示 RMS 本地已存在该业务键，本次重新导入并更新。
3. 第三方不能只依赖 `operation` 判断，应按 `businessKey` 执行 upsert，避免重试造成重复。

## 3. CONTRACT 合同数据

### 3.1 业务键

```json
{
  "contractId": "2026-0373",
  "fundId": "09900436"
}
```

| 字段 | 类型 | 说明 |
|---|---|---|
| `contractId` | string | 合同编号。 |
| `fundId` | string | 单个经费卡号。合同存在多个经费号时会拆成多条记录；无经费号时为空字符串。 |

唯一性规则：`contractId + fundId`。

### 3.2 data 字段

| 字段 | 类型 | 中文含义 |
|---|---|---|
| `contract_id` | string | 合同编号 |
| `contract_name` | string | 合同名称 |
| `leader` | string | 负责人 |
| `contract_funds` | number | 合同经费 |
| `contract_category` | string | 合同分类 |
| `effective_date` | string | 生效日期，`YYYY-MM-DD` |
| `undertaking_unit` | string | 所属单位 |
| `project_members` | string | 项目成员 |
| `registration_id` | string | 登记号 |
| `start_date` | string | 开始日期，`YYYY-MM-DD` |
| `end_date` | string | 终止日期，`YYYY-MM-DD` |
| `leader_type` | string | 负责人类型 |
| `leader_phone` | string | 负责人电话 |
| `leader_email` | string | 负责人邮箱 |
| `operator` | string | 经办人 |
| `operator_phone` | string | 经办人电话 |
| `validity_status` | string | 合同有效状态 |
| `progress_status` | string | 进行状态 |
| `payment_method` | string | 支付方式 |
| `party_a_sealed` | string | 甲方是否盖章 |
| `party_b_sealed` | string | 乙方是否盖章 |
| `contract_recovered` | string | 合同是否回收 |
| `statistical_attribution` | string | 统计归属 |
| `primary_discipline` | string | 一级学科 |
| `research_category` | string | 研究类别 |
| `cooperation_form` | string | 合作形式 |
| `project_source` | string | 项目来源 |
| `socioeconomic_target` | string | 社会经济服务目标 |
| `national_economy_industry` | string | 国民经济行业 |
| `audit_status` | string | 审核状态 |
| `remarks` | string | 备注 |
| `is_effective` | string | 合同是否生效 |
| `party_a_name` | string | 甲方名称 |
| `party_a_contact` | string | 甲方联系人 |
| `party_a_phone` | string | 甲方联系电话 |
| `party_a_type` | string | 甲方类型 |
| `party_a_province` | string | 甲方所属省份 |
| `party_a_city` | string | 甲方所属地市 |
| `party_a_postal_code` | string | 甲方邮编 |
| `party_a_address` | string | 甲方地址 |
| `patent_count` | integer | 专利件数 |
| `amount_received` | number | 到账金额 |
| `intellectual_property_number` | string | 专利（软著）号 |
| `leader_employee_id` | string | 负责人职工号 |
| `software_copyright_count` | integer | 软著件数 |
| `fund_id` | string | 经费卡号 |
| `conversion_type` | string | 转化类型 |
| `license_type` | string | 许可类型 |
| `purchase_funds` | number | 代购经费 |
| `cooperation_funds` | number | 合作经费 |

### 3.3 完整示例

```json
{
  "requestId": "6e547a55-8b25-41dc-bb24-fca9c71364fa",
  "dataType": "CONTRACT",
  "sentAt": "2026-08-22T10:30:00+08:00",
  "records": [
    {
      "operation": "INSERT",
      "businessKey": {
        "contractId": "2026-0373",
        "fundId": "09900436"
      },
      "data": {
        "contract_id": "2026-0373",
        "contract_name": "陶瓷焊接件内部残余应变分析合同",
        "leader": "示例负责人",
        "contract_funds": 5.2,
        "contract_category": "技术服务",
        "effective_date": "2026-03-27",
        "undertaking_unit": "工程技术研究院",
        "project_members": "示例成员",
        "registration_id": "",
        "start_date": "2024-12-02",
        "end_date": "2026-04-30",
        "leader_type": "教师",
        "leader_phone": "",
        "leader_email": "",
        "operator": "",
        "operator_phone": "",
        "validity_status": "已生效",
        "progress_status": "完成",
        "payment_method": "",
        "party_a_sealed": "",
        "party_b_sealed": "",
        "contract_recovered": "",
        "statistical_attribution": "科技类",
        "primary_discipline": "",
        "research_category": "",
        "cooperation_form": "",
        "project_source": "",
        "socioeconomic_target": "",
        "national_economy_industry": "",
        "audit_status": "终审通过",
        "remarks": "",
        "is_effective": "生效",
        "party_a_name": "示例单位",
        "party_a_contact": "",
        "party_a_phone": "",
        "party_a_type": "有限责任公司",
        "party_a_province": "北京市",
        "party_a_city": "北京市",
        "party_a_postal_code": "",
        "party_a_address": "",
        "patent_count": 0,
        "amount_received": 0.0,
        "intellectual_property_number": "",
        "leader_employee_id": "EMPLOYEE_ID",
        "software_copyright_count": 0,
        "fund_id": "09900436",
        "conversion_type": "",
        "license_type": "",
        "purchase_funds": 0.0,
        "cooperation_funds": 0.0
      }
    }
  ]
}
```

## 4. FUND_RECEIPT 经费到账数据

### 4.1 业务键

```json
{
  "unid": "d88eea1d-7513-45a7-b379-e44cf3d4027a"
}
```

`unid` 是 RMS 每次导入时为每笔到账生成的 UUID。经费到账按现有业务规则始终为 `INSERT`，第三方应按 `unid` 防止同一批重试产生重复。

### 4.2 data 字段

| 字段 | 类型 | 中文含义 |
|---|---|---|
| `unid` | string | 到账记录 UUID |
| `project_name` | string | 项目名称 |
| `fund_manager` | string | 经费卡负责人 |
| `funds_received` | number | 入账经费（万元） |
| `fund_id` | string | 经费账号 |
| `allocation_date` | string | 拨款日期，`YYYY-MM-DD` |
| `project_unit` | string | 项目所属单位 |
| `contract_id` | string | 项目编号 |
| `approval_number` | string | 批准号 |
| `approved_funds` | number | 批准（合同）经费（万元） |
| `manager_employee_id` | string | 经费卡负责人工号 |
| `project_leader` | string | 项目负责人 |
| `fund_unit` | string | 所在单位 |
| `project_category` | string | 项目分类 |
| `receipt_number` | string | 财务回单编号 |
| `retained_funds` | number | 留校金额（万元） |
| `allocated_funds` | number | 外拨金额 |
| `payment_unit` | string | 来款单位 |
| `payment_type` | string | 来款类型 |
| `audit_status` | string | 审核状态 |
| `project_nature` | string | 项目性质 |
| `project_level` | string | 项目级别 |

### 4.3 完整示例

```json
{
  "requestId": "38b218c1-d102-4d8e-af5d-6cf16630dace",
  "dataType": "FUND_RECEIPT",
  "sentAt": "2026-08-22T10:31:00+08:00",
  "records": [
    {
      "operation": "INSERT",
      "businessKey": {
        "unid": "d88eea1d-7513-45a7-b379-e44cf3d4027a"
      },
      "data": {
        "unid": "d88eea1d-7513-45a7-b379-e44cf3d4027a",
        "project_name": "示例项目",
        "fund_manager": "示例负责人",
        "funds_received": 5.0,
        "fund_id": "38320033",
        "allocation_date": "2026-03-31",
        "project_unit": "工程技术研究院",
        "contract_id": "2023-0199",
        "approval_number": "APPROVAL_NUMBER",
        "approved_funds": 10.0,
        "manager_employee_id": "EMPLOYEE_ID",
        "project_leader": "示例负责人",
        "fund_unit": "工程技术研究院",
        "project_category": "技术咨询",
        "receipt_number": "RECEIPT_NUMBER",
        "retained_funds": 5.0,
        "allocated_funds": 0.0,
        "payment_unit": "示例来款单位",
        "payment_type": "直接经费+间接经费",
        "audit_status": "终审通过",
        "project_nature": "横向",
        "project_level": ""
      }
    }
  ]
}
```

## 5. FUND_STATEMENT 经费收支数据

### 5.1 业务键

```json
{
  "fundId": "39320026",
  "periodStart": "2026-01-01",
  "periodEnd": "2026-03-31"
}
```

唯一性规则：`fundId + periodStart + periodEnd`。

### 5.2 汇总 data 字段

| 字段 | 类型 | 中文含义 |
|---|---|---|
| `fund_id` | string | 经费卡号 |
| `project_name` | string | 项目名称 |
| `period_start` | string | 统计起始日期，`YYYY-MM-DD` |
| `period_end` | string | 统计终止日期，`YYYY-MM-DD` |
| `opening_balance` | number | 期初余额 |
| `total_debit` | number | 借方累计发生额 |
| `total_credit` | number | 贷方累计发生额 |
| `ending_balance` | number | 期末余额 |
| `transactions` | array | 本期间流水；没有流水时为空数组 `[]`。 |

### 5.3 transactions 明细字段

| 字段 | 类型 | 中文含义 |
|---|---|---|
| `fund_id` | string | 经费卡号 |
| `sequence_number` | integer | 文件内顺序号，从1开始 |
| `transaction_date` | string | 交易日期，`YYYY-MM-DD` |
| `voucher_number` | string | 凭证号 |
| `summary` | string | 摘要 |
| `subject_code` | string | 科目代码 |
| `subject_name` | string | 科目名称 |
| `debit_amount` | number | 借方金额 |
| `credit_amount` | number | 贷方金额 |
| `balance` | number | 余额 |

流水唯一性规则：`fund_id + transaction_date + voucher_number + debit_amount + balance`。

### 5.4 有流水示例

```json
{
  "requestId": "0ec85a96-e877-4534-811d-e5eec4c96a2c",
  "dataType": "FUND_STATEMENT",
  "sentAt": "2026-08-22T10:32:00+08:00",
  "records": [
    {
      "operation": "INSERT",
      "businessKey": {
        "fundId": "39320026",
        "periodStart": "2026-01-01",
        "periodEnd": "2026-03-31"
      },
      "data": {
        "fund_id": "39320026",
        "project_name": "示例科研项目",
        "period_start": "2026-01-01",
        "period_end": "2026-03-31",
        "opening_balance": 100000.0,
        "total_debit": 15000.0,
        "total_credit": 0.0,
        "ending_balance": 85000.0,
        "transactions": [
          {
            "fund_id": "39320026",
            "sequence_number": 1,
            "transaction_date": "2026-01-15",
            "voucher_number": "VOUCHER_001",
            "summary": "测试费",
            "subject_code": "720102",
            "subject_name": "科研支出",
            "debit_amount": 15000.0,
            "credit_amount": 0.0,
            "balance": 85000.0
          }
        ]
      }
    }
  ]
}
```

### 5.5 零流水示例

零流水账户仍会推送汇总数据，但 `transactions` 必须为空数组，不发送虚拟占位流水。

```json
{
  "operation": "INSERT",
  "businessKey": {
    "fundId": "38320001",
    "periodStart": "2026-01-01",
    "periodEnd": "2026-03-31"
  },
  "data": {
    "fund_id": "38320001",
    "project_name": "示例项目",
    "period_start": "2026-01-01",
    "period_end": "2026-03-31",
    "opening_balance": 9.57,
    "total_debit": 0.0,
    "total_credit": 0.0,
    "ending_balance": 9.57,
    "transactions": []
  }
}
```

## 6. 响应报文

### 6.1 成功响应

第三方成功接收一批数据时必须返回 HTTP 200，响应体示例：

```json
{
  "code": 0,
  "message": "success",
  "requestId": "6e547a55-8b25-41dc-bb24-fca9c71364fa",
  "successCount": 2,
  "failedCount": 0,
  "results": [
    {
      "businessKey": {
        "contractId": "2026-0373",
        "fundId": "09900436"
      },
      "success": true,
      "targetId": "TARGET_ID",
      "message": ""
    }
  ]
}
```

| 字段 | 类型 | 必填 | 客户端判定 |
|---|---|---|---|
| `code` | integer | 是 | 必须等于 `0`。 |
| `message` | string | 否 | 响应说明。 |
| `requestId` | string | 建议 | 建议原样回传请求 `requestId`，便于双方对账。当前客户端不校验该字段。 |
| `successCount` | integer | 是 | 必须等于本批 `records` 数量。 |
| `failedCount` | integer | 是 | 必须等于 `0`。 |
| `results` | array | 否 | 建议返回逐条处理结果，便于排查；当前客户端不依赖该字段判定成功。 |

只有以下条件全部满足，RMS 才认为本批成功：

1. HTTP 状态码为 `200`。
2. 响应体是合法 JSON 对象。
3. `code == 0`。
4. `failedCount == 0`。
5. `successCount == 本批 records 数量`。

### 6.2 失败响应建议

参数错误或业务拒绝建议返回：

```json
{
  "code": 40001,
  "message": "contract_id is required",
  "requestId": "6e547a55-8b25-41dc-bb24-fca9c71364fa",
  "successCount": 0,
  "failedCount": 1,
  "results": [
    {
      "businessKey": {
        "contractId": "",
        "fundId": "09900436"
      },
      "success": false,
      "targetId": null,
      "message": "contract_id is required"
    }
  ]
}
```

第三方也可以对系统异常返回 HTTP 5xx。所有非 HTTP 200、非法 JSON、业务失败或部分失败都会触发整批重试。

## 7. 重试与幂等要求

RMS 客户端的重试行为：

| 项目 | 规则 |
|---|---|
| 首次请求超时 | 60秒 |
| 重试间隔 | 固定3秒 |
| 重试次数 | 首次失败后最多重试3次 |
| 最大请求次数 | 4次 |
| 重试请求内容 | 与首次请求完全一致 |
| 重试 requestId | 与首次请求完全一致 |

以下情况均触发重试：

- 网络连接异常或超时。
- HTTP 状态码不是 200。
- 响应体不是合法 JSON 对象。
- `code != 0`。
- `failedCount != 0`。
- `successCount` 与请求记录数不一致。

第三方幂等建议：

1. 记录已成功处理的 `requestId`。
2. 对同一个 `requestId` 的重复请求直接返回上一次成功结果，或安全地再次执行 upsert。
3. 合同按 `contractId + fundId` upsert。
4. 到账按 `unid` 去重。
5. 收支汇总按 `fundId + periodStart + periodEnd` upsert。
6. 收支流水按五字段唯一键 upsert。
7. 不得因客户端未收到响应而重复创建业务数据。

## 8. 数据格式约定

| 类型 | 约定 |
|---|---|
| 日期 | `YYYY-MM-DD` |
| 发送时间 | ISO 8601，包含时区 |
| 金额 | JSON number，不携带千分位和货币符号 |
| 整数 | JSON integer |
| 空字符串字段 | `""` |
| 空数字字段 | `0` 或 `0.0` |
| 空流水列表 | `[]` |
| 字符编码 | UTF-8 |

## 9. 联调检查清单

第三方接口准备完成后，双方按以下顺序联调：

1. 单条 `CONTRACT INSERT`。
2. 相同合同业务键的 `CONTRACT UPDATE`。
3. 单条 `FUND_RECEIPT INSERT`。
4. 有流水的 `FUND_STATEMENT`。
5. `transactions=[]` 的零流水 `FUND_STATEMENT`。
6. 同一 `requestId` 重复发送，确认没有重复数据。
7. 模拟 HTTP 500 后恢复，确认重试成功。
8. 模拟部分失败，确认整批被判定失败并重试。
9. 核对 `successCount` 与请求记录数。
10. 使用100条数据验证批量接收性能。

## 10. 第三方最低实现要求

- 提供 `POST /api/v1/rms/batch-import`。
- 接收 UTF-8 JSON。
- 支持三种 `dataType`。
- 单批支持至少100条记录。
- 依据 `businessKey` upsert 或去重。
- 依据 `requestId` 支持安全重试。
- 严格返回 `code`、`successCount` 和 `failedCount`。
- 处理成功后返回 HTTP 200。
- 接口处理时间应控制在60秒内。

---

**接口基准版本：** RMS SQLite v2 / 第三方批量推送 V1
**变更原则：** 字段删除、字段改名、业务键调整或响应规则变化均属于接口破坏性变更，双方确认后再实施。
