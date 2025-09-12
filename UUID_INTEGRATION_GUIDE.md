# UUID集成使用指南

## 概述

RMS系统已成功集成UUID支持，为每个数据表记录提供全局唯一标识符。本指南介绍UUID功能的使用方法和技术详情。

## UUID架构设计

### 设计方案
- **方案类型**: 保留自增ID + 添加UUID字段
- **UUID字段**: 所有表统一使用 `uuid` 字段名
- **索引策略**: UUID字段设置为UNIQUE索引，确保全局唯一性
- **生成时机**: 数据插入时自动生成

### 表结构变更

#### 1. contracts表
```sql
ALTER TABLE contracts ADD COLUMN uuid TEXT UNIQUE NOT NULL;
```

#### 2. projectfunds表  
```sql
ALTER TABLE projectfunds ADD COLUMN uuid TEXT UNIQUE NOT NULL;
```

#### 3. transactions表
```sql
ALTER TABLE transactions ADD COLUMN uuid TEXT UNIQUE NOT NULL;
```

## UUID功能特性

### 1. 自动生成
- 使用标准UUID4格式（36字符，包含连字符）
- 在数据插入时自动生成，无需手动指定
- 采用密码学安全的随机生成算法

### 2. 唯一性保证
- 全局唯一，跨表不重复
- 数据库级别UNIQUE约束
- 冲突检测和重试机制（极低概率事件）

### 3. 查询支持
- 支持通过UUID直接查询记录
- 支持批量UUID查询
- 支持UUID与业务主键的互相映射

## 使用方法

### 1. 数据插入
```python
# 数据插入时会自动生成UUID
db_manager = DatabaseManager()
data = {"contractid": "C001", "description": "测试合同"}
record_id = db_manager.insert_record("contracts", data)
# data字典会自动添加uuid字段
```

### 2. UUID查询
```python
# 通过UUID查询记录
record = db_manager.get_record_by_uuid("contracts", "12345678-1234-4567-8901-123456789abc")

# 批量UUID查询
uuid_list = ["uuid1", "uuid2", "uuid3"]
records = db_manager.batch_get_records_by_uuids("contracts", uuid_list)

# 通过业务主键获取UUID
uuid_value = db_manager.get_uuid_by_primary_keys("contracts", {"contractid": "C001"})
```

### 3. 配置使用
```python
# 配置文件已添加UUID字段支持
from config import CONTRACTS_FIELD_TYPES, CONTRACTS_COLUMN_MAPPING

# UUID字段类型配置
field_type = CONTRACTS_FIELD_TYPES["uuid"]  # "str"

# UUID字段映射配置  
english_name = CONTRACTS_COLUMN_MAPPING["uuid"]  # "uuid"
```

## OA同步集成

### 同步记录格式
```python
# 记录信息现在包含UUID
record_info = {
    "operation": "insert",
    "contractid": "C001", 
    "record_id": 123,
    "uuid": "12345678-1234-4567-8901-123456789abc",  # 新增UUID字段
    "data": {...}
}
```

### 同步优势
- 记录追踪：使用UUID进行精确的记录追踪
- 错误诊断：UUID帮助定位具体的同步问题
- 审计日志：UUID提供完整的操作审计链路

## 数据库管理

### 1. 表结构验证
```python
# 验证所有表是否包含UUID字段
db_manager = DatabaseManager()
validation_result = db_manager.validate_table_structure()
print(validation_result)
# {'contracts': True, 'projectfunds': True, 'transactions': True}
```

### 2. 数据库重建
```python
# 重建数据库表（清空所有数据）
db_manager.rebuild_database_tables(confirm_rebuild=True)

# 清空单个表数据
cleared_count = db_manager.clear_table_data("contracts", confirm_clear=True)
```

### 3. 数据迁移（如有现有数据）
由于采用重建表的方式，现有数据会被清空。重新导入Excel数据时将自动生成UUID。

## 技术实现细节

### 1. UUID生成函数
```python
def generate_uuid() -> str:
    """生成标准UUID4字符串"""
    return str(uuid.uuid4())

def validate_uuid(uuid_string: str) -> bool:
    """验证UUID字符串格式"""
    try:
        uuid.UUID(uuid_string)
        return True
    except (ValueError, TypeError):
        return False
```

### 2. 数据库约束
- UUID字段设置为NOT NULL
- UUID字段设置为UNIQUE索引
- 保留原有的业务主键约束

### 3. 性能考虑
- UUID索引：优化UUID查询性能
- 查询策略：优先使用业务主键，UUID作为补充
- 批量操作：支持批量UUID查询，减少数据库往返

## 日志和监控

### 1. 操作日志
所有数据库操作都会记录UUID信息，便于问题追踪：
```
数据库操作 - 插入 - contracts | 影响行数: 1 | uuid=12345678-1234-4567-8901-123456789abc
```

### 2. OA同步日志
OA同步操作包含UUID信息，提高可追溯性：
```
OA操作 - 同步合同数据: table=contracts | uuid=12345678-1234-4567-8901-123456789abc
```

## 故障排除

### 1. UUID格式错误
```python
# 错误示例
invalid_uuid = "invalid-uuid-format"
try:
    record = db_manager.get_record_by_uuid("contracts", invalid_uuid)
except ValueError as e:
    print(f"UUID格式错误: {e}")
```

### 2. UUID冲突（极少发生）
```python
# 系统会自动重试生成新的UUID
try:
    new_uuid = generate_unique_uuid(existing_uuids_set)
except RuntimeError as e:
    print(f"UUID生成失败: {e}")
```

### 3. 查询不到记录
```python
# UUID查询返回None表示记录不存在
record = db_manager.get_record_by_uuid("contracts", some_uuid)
if record is None:
    print("指定UUID的记录不存在")
```

## 最佳实践

### 1. UUID使用建议
- 优先使用业务主键进行常规查询
- 使用UUID进行跨系统数据追踪
- 在错误日志中包含UUID便于问题定位

### 2. 性能优化
- 避免在大量数据操作中频繁使用UUID查询
- 使用批量UUID查询替代多次单独查询
- 定期监控UUID索引的使用情况

### 3. 数据备份
- 备份时确保包含UUID字段
- 恢复数据时验证UUID完整性
- 跨环境迁移时保持UUID一致性

## 版本兼容性

### 向后兼容
- 现有代码无需修改即可继续工作
- 自增ID主键功能完全保留
- 原有的业务逻辑不受影响

### 向前扩展
- 为未来分布式部署奠定基础
- 支持跨系统数据同步和追踪
- 便于系统集成和数据交换

## 更新说明

- **版本**: 2025年1月7日更新
- **变更类型**: 新增功能
- **影响范围**: 数据库表结构、数据操作、OA同步
- **兼容性**: 向后兼容，无破坏性变更