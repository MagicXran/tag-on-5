# 收支明细表处理器使用说明

## 功能概述

收支明细表处理器用于从指定文件夹中读取 Excel 格式的收支明细表，进行数据解析、清理、保存到数据库，并与 OA 系统同步。

## 主要特性

- **批量处理**: 自动扫描文件夹中的所有 Excel 文件
- **智能解析**: 自动解析经费卡号、项目名称、起始/终止日期等信息
- **数据清理**: 自动进行数据类型转换和无效值处理
- **数据库操作**: 支持插入和更新操作，避免重复数据
- **OA 同步**: 支持主从表模式向 OA 系统同步数据
- **完善日志**: 详细记录处理过程和错误信息

## Excel 文件格式要求

### 文件结构

- **第 2 行 C2-H2**: 经费卡号信息（合并单元格）
- **第 3 行**: 字段名称行
- **第 4 行**: 跳过行（略过不处理）
- **第 5 行开始**: 实际收支明细数据
- **汇总行**: 包含"累计发生额"和"期末余额"的行

### 经费卡号信息格式

```
39320284（有预算）钛合金薄带轧制成形关键技术及产 起始日期：2025-01-01 终止日期：2025-03-26
```

格式解析规则：

- 开头数字：经费卡号（必需）
- 括号内容：可选，如"（有预算）"
- 项目名称：经费卡号后的文本（必需）
- 起始日期：可选，格式"起始日期：YYYY-MM-DD"
- 终止日期：可选，格式"终止日期：YYYY-MM-DD"

### 字段定义（第 3 行）

| 列  | 字段名称 | 数据类型 | 说明         |
| --- | -------- | -------- | ------------ |
| A   | 日期     | datetime | 交易日期     |
| B   | 凭证号   | str      | 财务凭证号   |
| C   | 摘要     | str      | 交易摘要     |
| D   | 科目代码 | str      | 会计科目代码 |
| E   | 科目名称 | str      | 会计科目名称 |
| F   | 借方金额 | float    | 借方金额     |
| G   | 贷方金额 | float    | 贷方金额     |
| H   | 余额     | float    | 账户余额     |

### 汇总信息

- **累计发生额行**: 摘要列包含"累计发生额"，读取 F 列（借方累计）和 G 列（贷方累计）
- **期末余额行**: 摘要列包含"期末余额"，读取 H 列（期末余额）

## 安装和配置

### 1. 环境要求

- Python 3.7+
- 依赖包：pandas, pymysql, aiohttp, requests

### 2. 安装依赖

```bash
pip install -r requirements.txt
```

### 3. 配置文件设置

在 `config.py` 中配置：

```python
# Excel文件路径配置
EXCEL_FILES = {
    "transactions": r"X:\Nercar\RMS\rms-tag-on\收支明细表文件夹"
}

# MySQL数据库配置
MYSQL_CONFIG = {
    "user": "root",
    "password": "rms",
    "host": "localhost",
    "database": "rms",
    "charset": "utf8mb4",
    "port": 3306
}

# OA系统配置
OA_CONFIG = {
    "base_url": "http://10.0.3.39",
    "login_name": "admin",
    "rest_user": "beike",
    "rest_pass": "de900bd7-dd69-4002-83e1-88e5acfe3ccd",
    "timeout": 30,
    "max_retries": 3
}

# 运行时配置
RUNTIME_CONFIG = {
    "enable_oa_sync": True,              # 是否启用OA同步
    "enable_oa_master_sub_table": True,  # 是否启用OA主从表
    "batch_size": 100,
    "max_workers": 5,
    "connection_pool_size": 10
}
```

## 使用方法

### 1. 命令行运行

```bash
# 使用配置文件中的路径
python run_transactions_processor.py

# 指定文件夹路径
python run_transactions_processor.py --folder /path/to/transactions/folder

# 详细输出模式
python run_transactions_processor.py --verbose

# 查看帮助
python run_transactions_processor.py --help
```

### 2. 编程方式调用

```python
from transactions_processor import TransactionsProcessor

# 创建处理器实例
processor = TransactionsProcessor()

# 处理指定文件夹
results = processor.process_transactions_folder("/path/to/folder")

# 查看处理结果
print(f"处理文件数: {results['processed_files']}")
print(f"成功记录数: {results['successful_records']}")
print(f"失败记录数: {results['failed_records']}")
```

### 3. 测试功能

```bash
# 运行测试脚本
python test_transactions_processor.py
```

## 数据库表结构

### transactions 表

| 字段名                 | 中文名称       | 数据类型 | 说明             |
| ---------------------- | -------------- | -------- | ---------------- |
| fundid                 | 经费卡号       | VARCHAR  | 主键之一         |
| transactiondate        | 日期           | DATE     | 主键之一         |
| vouchernumber          | 凭证号         | VARCHAR  | 主键之一         |
| description            | 摘要           | VARCHAR  | 交易描述         |
| subjectcode            | 科目代码       | VARCHAR  | 会计科目代码     |
| subjectname            | 科目名称       | VARCHAR  | 会计科目名称     |
| debitamount            | 借方金额       | DECIMAL  | 借方金额         |
| creditamount           | 贷方金额       | DECIMAL  | 贷方金额         |
| balance                | 余额           | DECIMAL  | 账户余额         |
| closingbalance         | 期末余额       | DECIMAL  | 期末余额         |
| cumulativeDebitAmount  | 借方累计发生额 | DECIMAL  | 借方累计金额     |
| cumulativeCreditAmount | 贷方累计发生额 | DECIMAL  | 贷方累计金额     |
| transname              | 项目名称       | VARCHAR  | 项目名称         |
| updateid               | OA 同步 ID     | VARCHAR  | OA 系统返回的 ID |

## OA 系统同步

### 主从表结构

- **主表**: formmain_0018 (收支明细主表)
- **从表**: formson_0019 (收支明细子表)
- **分组字段**: fundid (经费卡号)

### 字段映射

| 数据库字段             | OA 字段编码 | 中文名称       |
| ---------------------- | ----------- | -------------- |
| fundid                 | field0001   | 经费卡号       |
| transname              | field0002   | 项目名称       |
| cumulativeDebitAmount  | field0003   | 借方累计发生额 |
| cumulativeCreditAmount | field0004   | 贷方累计发生额 |
| closingbalance         | field0005   | 期末余额       |
| sequence_number        | field0006   | 序号 1         |
| transactiondate        | field0007   | 日期           |
| vouchernumber          | field0008   | 凭证号         |
| description            | field0009   | 摘要           |
| subjectcode            | field0010   | 科目代码       |
| subjectname            | field0011   | 科目名称       |
| debitamount            | field0012   | 借方金额       |
| creditamount           | field0013   | 贷方金额       |
| balance                | field0014   | 余额           |
| contractid             | field0015   | 项目编号       |

### 同步逻辑

1. 按经费卡号分组记录
2. 每组第一条记录作为主表记录
3. 其余记录作为子表记录
4. 排除字段：fundid, transname（在子表中不重复）

## 日志系统

### 日志文件位置

- 默认位置: `logs/` 目录
- 文件名格式: `rms_tag_on_YYYYMMDD.log`

### 日志级别

- **INFO**: 正常处理信息
- **WARNING**: 警告信息（如数据转换失败）
- **ERROR**: 错误信息（如文件解析失败）
- **DEBUG**: 详细调试信息（需要指定 --verbose 参数）

## 错误处理

### 常见错误及解决方案

1. **文件夹不存在**

   - 检查配置文件中的路径设置
   - 确保文件夹路径正确且可访问

2. **Excel 文件格式不正确**

   - 检查第 2 行 C2-H2 是否为经费卡号信息
   - 确认第 3 行是否为字段名称行
   - 验证数据从第 5 行开始

3. **经费卡号解析失败**

   - 检查经费卡号是否以数字开头
   - 确认格式符合要求

4. **数据库连接失败**

   - 检查数据库配置信息
   - 确认数据库服务正在运行
   - 验证用户权限

5. **OA 同步失败**
   - 检查 OA 系统配置信息
   - 确认网络连接正常
   - 验证 REST API 凭据

## 性能优化

### 批处理设置

在 `config.py` 中调整批处理参数：

```python
RUNTIME_CONFIG = {
    "batch_size": 100,          # 批处理大小
    "max_workers": 5,           # 最大工作线程数
    "connection_pool_size": 10  # 数据库连接池大小
}
```

### 大文件处理建议

- 对于包含大量数据的 Excel 文件，建议适当增加批处理大小
- 如果内存有限，可以减少 max_workers 数量
- 定期清理日志文件以节省磁盘空间

## 维护和监控

### 定期检查

1. 监控日志文件，关注错误和警告信息
2. 检查数据库表的数据完整性
3. 验证 OA 系统同步状态

### 备份建议

1. 定期备份数据库
2. 保留原始 Excel 文件
3. 备份配置文件

## 技术支持

如遇到问题，请：

1. 查看日志文件中的详细错误信息
2. 使用 `--verbose` 参数获取更多调试信息
3. 运行测试脚本验证系统状态
4. 检查配置文件设置是否正确
