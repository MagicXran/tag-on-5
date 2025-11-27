# RMS 标签导入处理系统

## 系统概述

RMS 标签导入处理系统是一个完整的数据处理解决方案，用于从 Excel 文件读取合同签订清单和经费到账清单数据，进行数据清洗、筛选后保存到 MySQL 数据库，并通过 OA 接口同步数据到 OA 系统。

## 功能特性

- 📊 **Excel 数据处理**: 支持读取和解析.xls 格式的 Excel 文件
- 🧹 **数据清洗**: 自动数据类型转换、空值处理、数据验证
- 🔍 **智能筛选**: 基于业务规则的数据筛选和过滤
- 🗄️ **数据库操作**: 自动判断插入或更新数据库记录
- 🌐 **OA 系统集成**: 异步同步数据到 OA 系统
- 📝 **完整日志**: 每日日志文件，记录所有操作详情
- ⚡ **高性能**: 批量处理、异步操作、连接池管理

## 系统架构

```
tag-on-5/
├── config.py              # 系统配置文件
├── logger_utils.py         # 日志管理模块
├── data_cleaner.py         # 数据清洗模块
├── database_manager.py     # 数据库管理模块
├── oa_sync_manager.py      # OA同步管理模块
├── main_processor.py       # 主处理器模块
├── run_processor.py        # 主入口文件
├── test_config.py          # 配置测试脚本
└── README.md              # 系统说明文档
```

## 安装要求

### Python 版本

- Python 3.7 或更高版本

### 依赖模块

```bash
pip install pandas pymysql aiohttp xlrd openpyxl
```

## 配置说明

### 1. Excel 文件配置

在 `config.py` 中设置 Excel 文件路径：

```python
EXCEL_FILES = {
    "contracts": r"X:\path\to\合同签订清单_横向_20250425_田文月给.xls",
    "project_funds": r"X:\path\to\经费到账清单_横向+纵向_20250425_田文月给.xls",
}
```

### 2. 数据库配置

```python
MYSQL_CONFIG = {
    "user": "root",
    "password": "your_password",
    "host": "localhost",
    "database": "rms",
    "charset": "utf8mb4",
    "port": 3306
}
```

### 3. OA 系统配置

```python
OA_CONFIG = {
    "base_url": "http://10.0.3.39",
    "login_name": "admin",
    "rest_user": "beike",
    "rest_pass": "your_oa_password",
    "timeout": 30,
    "max_retries": 3
}
```

## 使用方法

### 1. 环境检查

```bash
python test_config.py
```

### 2. 运行系统

```bash
python run_processor.py
```

### 3. 查看帮助

```bash
python run_processor.py --help
```

### 4. 仅检查环境

```bash
python run_processor.py --check
```

## 数据处理流程

### 合同签订清单处理

1. **数据读取**: 从 Excel 文件读取合同数据
2. **数据清洗**: 类型转换、空值处理
3. **业务筛选**:
   - 合同编号不为空
   - 所属单位为"工程技术研究院"或"冶金工程研究院"
   - 或者经费卡号以 3932/3832/3934 开头
4. **数据库操作**: 以 contractid 为主键进行插入或更新
5. **OA 同步**: 异步同步到 OA 系统

### 经费到账清单处理

1. **数据读取**: 从 Excel 文件读取经费数据
2. **数据清洗**: 类型转换、空值处理
3. **业务筛选**:
   - 经费账号不为空
   - 项目所属单位或所在单位为"工程技术研究院"或"冶金工程研究院"
   - 入账经费和项目编号不为空
4. **数据库操作**: 以 fundid+funds_received+contractid 为联合主键进行插入或更新
5. **OA 同步**: 异步同步到 OA 系统

## 字段映射

### 合同字段映射示例

| 中文字段名 | 英文字段名    | 数据类型 |
| ---------- | ------------- | -------- |
| 合同编号   | contractid    | str      |
| 合同名称   | description   | str      |
| 负责人     | leader        | str      |
| 合同经费   | contractfunds | float    |
| 生效日期   | signdate      | datetime |

### 经费字段映射示例

| 中文字段名       | 英文字段名      | 数据类型 |
| ---------------- | --------------- | -------- |
| 项目名称         | project_name    | str      |
| 经费账号         | fundid          | str      |
| 入账经费（万元） | funds_received  | float    |
| 项目编号         | contractid      | str      |
| 拨款时间         | allocation_date | datetime |

## 日志文件

系统会在 `logs/` 目录下生成每日日志文件：

- 文件格式: `rms_tag_on_YYYYMMDD.log`
- 记录内容: 数据处理步骤、统计信息、错误信息、OA 同步状态

## 错误处理

系统包含完善的错误处理机制：

- **文件不存在**: 自动检查 Excel 文件路径
- **数据库连接**: 自动重试和连接池管理
- **数据格式错误**: 类型转换失败时使用默认值
- **OA 接口异常**: 重试机制和详细错误日志
- **网络超时**: 可配置的超时时间和重试次数

## 性能优化

- **批量处理**: 数据库批量插入/更新操作
- **异步操作**: OA 接口异步调用
- **连接池**: 数据库连接池管理
- **内存管理**: 大文件分块处理
- **缓存机制**: 配置信息缓存

## 安全特性

- **密码保护**: 配置文件中的敏感信息可加密存储
- **参数校验**: 所有输入参数都经过验证
- **SQL 注入防护**: 使用参数化查询
- **连接安全**: 数据库连接使用 SSL（可选）

## 故障排除

### 常见问题

1. **模块导入错误**

   ```bash
   pip install -r requirements.txt
   ```

2. **Excel 文件读取失败**

   - 检查文件路径是否正确
   - 确认文件格式为.xls
   - 检查文件是否被其他程序占用

3. **数据库连接失败**

   - 验证数据库连接信息
   - 检查数据库服务是否运行
   - 确认用户权限

4. **OA 接口调用失败**
   - 检查网络连接
   - 验证 OA 系统配置信息
   - 查看详细错误日志

### 调试模式

设置详细日志级别进行调试：

```python
LOG_CONFIG = {
    "log_level": "DEBUG"
}
```

## 版本信息

- **当前版本**: v1.0.0
- **Python 要求**: >=3.7
- **最后更新**: 2025-07-8

## 技术支持

如遇技术问题，请：

1. 检查日志文件中的错误信息
2. 运行配置测试脚本
3. 查看系统运行环境

---

_RMS 标签导入处理系统为科研数据管理提供可靠的自动化解决方案_
