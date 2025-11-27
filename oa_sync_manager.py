"""
OA同步管理模块 - 负责与OA系统的数据同步
基于new_main.py中的OAImporter逻辑进行重构和改进
"""

import requests
import asyncio
import aiohttp
from typing import Dict, List, Optional, Any
from config import OA_CONFIG
from logger_utils import process_logger
import random

class OASyncManager:
    """OA同步管理器，负责与OA系统的异步数据交互"""
    
    """
    
    表单编号

科研合同档案： keyanhetong
经费到账： jingfeidaozhang
经费收支档案： jingfeishouzhi
    
    """
    # OA表配置信息
    TABLE_INFO = {
        "contracts": {
            "masterTable": "formmain_20961",
            "subTable": None,
            # "rightId": "8738386221147704578.4686476435574376301",
            "rightId": "-3987276264785230250.-5526404755293779830",
            "formCode": "keyanhetong"
        },
        "projectfunds": {
            "masterTable": "formmain_20960",
            "subTable": None,
            # "rightId": "-8468617479218967734.-5627575296880786341",
            "rightId": "8275895863371317926.5581798851522481528",
            "formCode": "jingfeidaozhang"
        },
        "transactions": {
            "masterTable": "formmain_20958",
            "subTable": "formson_20959",
            # "rightId": "-5159805959137505715.-6609998234064302859",
            "rightId": "-5360477720843995010.1941665291310747000",
            "formCode": "jingfeishouzhi",
            "groupKey": "fundid",
            "excludeFields": ["fundid", "transname"]
        }
    }
    
    # 字段映射：本地数据库列名 -> OA字段编码
    FIELD_MAPPINGS = {
        "contracts": {
            "contractid": "field0001",
            "description": "field0002",
            "leader": "field0003",
            "contractfunds": "field0004",
            "contractclassification": "field0005",
            "signdate": "field0006",
            "undertakingunit": "field0007",
            "projectmembers": "field0008",
            "registrationid": "field0009",
            "startdate": "field0010",
            "enddate": "field0011",
            "leadertype": "field0012",
            "leadertelephone": "field0013",
            "leaderemail": "field0014",
            "operator": "field0015",
            "operatortelephone": "field0016",
            "contracteffectivestatus": "field0017",
            "contractstatus": "field0018",
            "paymentmode": "field0019",
            "partyaseal": "field0020",
            "partybseal": "field0021",
            "contractrecovered": "field0022",
            "statisticalattribution": "field0023",
            "subjectclassification": "field0024",
            "researchcategory": "field0025",
            "formscooperation": "field0026",
            "projectsource": "field0027",
            "socialeconomictarget": "field0028",
            "neic": "field0029",
            "auditstatus": "field0030",
            "remarks": "field0031",
            "iseffective": "field0032",
            "partyaname": "field0033",
            "partyacontact": "field0034",
            "partyatel": "field0035",
            "partyatype": "field0036",
            "partaprovince": "field0037",
            "partyacity": "field0038",
            "partapostalcode": "field0039",
            "partaaddress": "field0040",
            "patentcount": "field0041",
            "amountreceived": "field0042",
            "copyrightid": "field0043",
            "manageremployeeid": "field0044",
            "copyrightcount": "field0045",
            "fundids": "field0046",
            "conversiontype": "field0048",
            "licensetype": "field0049",
            "department": "field0050"
        },
        "projectfunds": {
            "project_name": "field0001",
            "fundid": "field0002",
            "fund_manager": "field0003",
            "funds_received": "field0004",
            "allocation_date": "field0005",
            "project_unit": "field0006",
            "contractid": "field0007",
            "approved_funds": "field0008",
            "manager_id": "field0009",
            "project_leader": "field0010",
            "fund_unit": "field0011",
            "project_category": "field0012",
            "receipt_id": "field0013",
            "retained_funds": "field0014",
            "allocated_funds": "field0015",
            "payment_unit": "field0016",
            "payment_type": "field0017",
            "audit_status": "field0018",
            "project_nature": "field0019",
            "project_level": "field0020",
            "unid": "field0024"  # 单据号（系统生成UUID）
        },
        "transactions": {
            "fundid": "field0001",
            "transname": "field0002",
            "cumulativeDebitAmount": "field0003",
            "cumulativeCreditAmount": "field0004",
            "closingbalance": "field0005",
            "sequence_number": "field0006",
            "transactiondate": "field0007",
            "vouchernumber": "field0008",
            "description": "field0009",
            "subjectcode": "field0010",
            "subjectname": "field0011",
            "debitamount": "field0012",
            "creditamount": "field0013",
            "balance": "field0014",
            "contractid": "field0015"
        }
    }
    
    def __init__(self, config: Dict = None, enable_master_sub_table: bool = False):
        self.config = config or OA_CONFIG
        self.base_url = self.config['base_url'].rstrip('/')
        self.login_name = self.config['login_name']
        self.rest_user = self.config['rest_user']
        self.rest_pass = self.config['rest_pass']
        self.enable_sync = self.config.get('enable_oa_sync', True)
        self.enable_master_sub_table = enable_master_sub_table
    
    async def get_token(self) -> Optional[str]:
        """异步获取OA访问令牌
        
        Returns:
            访问令牌或None
        """
        if not self.rest_user or not self.rest_pass:
            process_logger.log_error("OA配置", "REST用户凭据未配置")
            return None
        
        url = f"{self.base_url}/seeyon/rest/token"
        # url = f"{self.base_url}/seeyon/keyanzhanghao/token"
        data = {
            "userName": self.rest_user,
            "password": self.rest_pass
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=data) as response:
                    if response.status == 200:
                        result = await response.json()
                        token = result.get('id')
                        if token:
                            process_logger.log_oa_operation("获取令牌", status="成功", token=token[:10] + "...")
                            return token
                        else:
                            process_logger.log_error("OA令牌", "响应中无令牌", response=result)
                    else:
                        text = await response.text()
                        process_logger.log_error("OA令牌", f"HTTP {response.status}", response=text[:200])
        except Exception as e:
            process_logger.log_error("OA令牌", str(e))
        
        return None
    
    def format_record_data(self, record_info: Dict, table_name: str, exclude_fields: List[str] = None) -> Dict:
        """格式化记录数据为OA接口格式
        
        Args:
            record_info: 记录信息
            table_name: 表名
            exclude_fields: 需要排除的字段列表
            
        Returns:
            格式化后的记录数据
        """
        data = record_info['data']
        operation = record_info['operation']
        exclude_fields = exclude_fields or []
        
        # 构建字段列表
        fields = []
        field_mapping = self.FIELD_MAPPINGS.get(table_name, {})
        
        for db_field, value in data.items():
            # 跳过排除的字段
            if db_field in exclude_fields:
                continue
                
            if db_field in field_mapping:
                oa_field = field_mapping[db_field]
                
                # 值转换
                if value is None:
                    value = ""
                elif hasattr(value, 'strftime'):
                    value = value.strftime("%Y-%m-%d %H:%M:%S")
                else:
                    value = str(value)
                
                fields.append({
                    "name": oa_field,
                    "value": value,
                    "showValue": db_field  # 用于调试
                })
        
        # 构建记录结构
        record = {
            "fields": fields
        }
        
        # 设置记录ID
        if operation == "update" and record_info.get("updateid"):
            record["id"] = record_info["updateid"]
        else:
            # 对于新插入的记录，使用本地记录ID
            record["id"] = record_info.get("record_id", 1)
            # record["id"] = random.randint(1000000000,9999999999)
        
        return record
    
    def build_oa_request_payload(self, records: List[Dict], table_name: str, operation: str = "add") -> Dict:
        """构建OA接口请求载荷
        
        Args:
            records: 记录列表
            table_name: 表名
            operation: 操作类型 (add/update)
            
        Returns:
            请求载荷
        """
        table_info = self.TABLE_INFO.get(table_name)
        if not table_info:
            raise ValueError(f"未配置的表: {table_name}")
        
        # 检查是否需要主从表处理
        if self.enable_master_sub_table and table_info.get("subTable"):
            return self.build_master_sub_table_payload(records, table_name, operation)
        else:
            return self.build_simple_table_payload(records, table_name, operation)
    
    def build_simple_table_payload(self, records: List[Dict], table_name: str, operation: str = "add") -> Dict:
        """构建简单表的请求载荷"""
        table_info = self.TABLE_INFO.get(table_name)
        data_list = []
        
        for record_info in records:
            formatted_record = self.format_record_data(record_info, table_name)
            
            entry = {
                "masterTable": {
                    "name": table_info["masterTable"],
                    "record": formatted_record,
                    "changedFields": []
                },
                "subTables": []
            }
            
            data_list.append(entry)
        
        payload = {
            "formCode": table_info["formCode"],
            "loginName": self.login_name,
            "rightId": table_info["rightId"],
            "doTrigger": "true",
            "dataList": data_list
        }
        
        return payload
    
    def build_master_sub_table_payload(self, records: List[Dict], table_name: str, operation: str = "add") -> Dict:
        """构建主从表的请求载荷"""
        table_info = self.TABLE_INFO.get(table_name)
        group_key = table_info.get("groupKey")
        exclude_fields = table_info.get("excludeFields", [])
        
        if not group_key:
            raise ValueError(f"主从表{table_name}缺少groupKey配置")
        
        # 按分组键对记录进行分组
        groups = {}
        for record_info in records:
            data = record_info['data']
            key_value = data.get(group_key)
            if key_value:
                if key_value not in groups:
                    groups[key_value] = []
                groups[key_value].append(record_info)
        
        data_list = []
        
        for key_value, group_records in groups.items():
            # 第一条记录作为主表记录
            master_record_info = group_records[0]
            master_record = self.format_record_data(master_record_info, table_name)
            
            # 其余记录作为子表记录
            sub_records = []
            for sub_record_info in group_records[1:]:
                sub_record = self.format_record_data(
                    sub_record_info, table_name, exclude_fields=exclude_fields
                )
                sub_records.append(sub_record)
            
            entry = {
                "masterTable": {
                    "name": table_info["masterTable"],
                    "record": master_record,
                    "changedFields": []
                },
                "subTables": []
            }
            
            if sub_records:
                entry["subTables"].append({
                    "name": table_info["subTable"],
                    "records": sub_records,
                    "changedFields": []
                })
            
            data_list.append(entry)
        
        payload = {
            "formCode": table_info["formCode"],
            "loginName": self.login_name,
            "rightId": table_info["rightId"],
            "doTrigger": "true",
            "dataList": data_list
        }
        
        return payload
    
    async def send_to_oa(self, records: List[Dict], table_name: str, operation: str = "add") -> Optional[Dict]:
        """异步发送数据到OA系统
        
        Args:
            records: 记录列表
            table_name: 表名
            operation: 操作类型
            
        Returns:
            OA响应结果
        """
        if not self.enable_sync:
            process_logger.log_oa_operation("跳过OA同步", reason="同步已禁用")
            return None
        
        if not records:
            process_logger.log_oa_operation("跳过OA同步", reason="无记录需要同步")
            return None
        
        try:
            # 获取令牌
            token = await self.get_token()
            if not token:
                process_logger.log_error("OA同步", "无法获取访问令牌")
                return None
            
            # 构建请求数据
            payload = self.build_oa_request_payload(records, table_name, operation)
            
            # 发送请求
            url = f"{self.base_url}/seeyon/rest/cap4/form/soap/batch-{operation}"
            headers = {
                "token": token,
                "Content-Type": "application/json"
            }
            
            process_logger.log_oa_operation(
                f"发送{operation}请求",
                table=table_name,
                record_count=len(records),
                url=url,
                payload=payload,
                headers=headers
            )
            
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload, headers=headers, timeout=30) as response:
                    if response.status == 200:
                        result = await response.json()
                        process_logger.log_oa_operation(
                            f"{operation}响应",
                            status="成功",
                            table=table_name,
                            response_keys=list(result.keys())
                        )
                        return result
                    else:
                        text = await response.text()
                        process_logger.log_error(
                            "OA同步",
                            f"HTTP {response.status}",
                            table=table_name,
                            response=text[:200]
                        )
                        return None
        
        except Exception as e:
            process_logger.log_error("OA同步", str(e), table=table_name, operation=operation)
            return None
    
    def extract_success_ids(self, oa_response: Dict) -> List[str]:
        """从OA响应中提取成功的ID列表
        
        Args:
            oa_response: OA响应数据
            
        Returns:
            成功ID列表
        """
        if not oa_response:
            return []
        
        data = oa_response.get("data", {})
        success_ids = data.get("successIdList", [])
        
        if success_ids:
            process_logger.log_oa_operation(
                "提取成功ID",
                count=len(success_ids),
                ids=success_ids[:3]  # 只记录前3个ID用于调试
            )
        
        return success_ids
    
    async def sync_records_to_oa(self, inserted_records: List[Dict], updated_records: List[Dict], table_name: str) -> Dict[str, List[str]]:
        """同步记录到OA系统
        
        Args:
            inserted_records: 新插入的记录列表
            updated_records: 更新的记录列表
            table_name: 表名
            
        Returns:
            同步结果字典 {"insert_ids": [...], "update_ids": [...]}
        """
        result = {"insert_ids": [], "update_ids": []}
        
        # 同步新增记录
        if inserted_records:
            process_logger.log_start(
                f"同步新增记录到OA",
                table=table_name,
                count=len(inserted_records)
            )
            
            print(f"=== OA同步调试 - 新增记录 ===")
            print(f"表名: {table_name}")
            print(f"新增记录数: {len(inserted_records)}")
            
            # 包装记录为OA期望的格式
            wrapped_insert_records = []
            for i, record in enumerate(inserted_records):
                # 调试输出原始记录
                print(f"新增记录 {i+1}:")
                print(f"  原始record: {record}")
                
                # 从record结构中正确提取数据
                if isinstance(record, dict) and "data" in record:
                    # 新格式：record = {"operation": "insert", "data": {...}, ...}
                    record_data = record.get("data", {})
                    record_id = record.get("record_id")
                    updateid = record.get("updateid")
                else:
                    # 旧格式：record直接是数据字典
                    record_data = record
                    record_id = record.get("id") or record.get("ID")
                    updateid = record.get("updateid") or record.get("UpdateId")
                
                wrapped_record = {
                    "data": record_data,
                    "operation": "add",
                    "record_id": record_id,
                    "updateid": updateid
                }
                
                print(f"  包装后记录: {wrapped_record}")
                wrapped_insert_records.append(wrapped_record)
            
            insert_response = await self.send_to_oa(wrapped_insert_records, table_name, "add")
            print(f"  OA插入响应: {insert_response}")
            
            if insert_response:
                insert_ids = self.extract_success_ids(insert_response)
                result["insert_ids"] = insert_ids
                print(f"  提取的插入IDs: {insert_ids}")
        
        # 同步更新记录
        if updated_records:
            process_logger.log_start(
                f"同步更新记录到OA",
                table=table_name,
                count=len(updated_records)
            )
            
            print(f"=== OA同步调试 - 更新记录 ===")
            print(f"表名: {table_name}")
            print(f"更新记录数: {len(updated_records)}")
            
            # 包装记录为OA期望的格式
            wrapped_update_records = []
            for i, record in enumerate(updated_records):
                # 调试输出原始记录
                print(f"更新记录 {i+1}:")
                print(f"  原始record: {record}")
                
                # 从record结构中正确提取数据
                if isinstance(record, dict) and "data" in record:
                    # 新格式：record = {"operation": "update", "data": {...}, "updateid": "...", ...}
                    record_data = record.get("data", {})
                    record_id = record_data.get("id") or record_data.get("ID") 
                    updateid = record.get("updateid")  # 从record顶层获取updateid
                    
                    print(f"  提取的数据:")
                    print(f"    - record_data keys: {list(record_data.keys())}")
                    print(f"    - record_id: {record_id}")
                    print(f"    - updateid: {updateid}")
                    
                    # 检查关键字段
                    if table_name == "contracts":
                        contractfunds = record_data.get("contractfunds")
                        print(f"    - contractfunds: {contractfunds}")
                    
                else:
                    # 旧格式：record直接是数据字典
                    record_data = record
                    record_id = record.get("id") or record.get("ID")
                    updateid = record.get("updateid") or record.get("UpdateId")
                    print(f"  旧格式记录，直接使用")
                
                # 检查updateid是否为空
                if updateid is None:
                    print(f"  警告: updateid为None，OA更新可能失败")
                    # 为了调试，我们仍然尝试发送
                    print(f"  继续尝试发送以便调试...")
                
                wrapped_record = {
                    "data": record_data,
                    "operation": "update",
                    "record_id": record_id,
                    "updateid": updateid
                }
                
                print(f"  包装后记录: {wrapped_record}")
                wrapped_update_records.append(wrapped_record)
            
            print(f"发送到OA的更新记录数: {len(wrapped_update_records)}")
            update_response = await self.send_to_oa(wrapped_update_records, table_name, "update")
            print(f"OA更新响应: {update_response}")
            
            if update_response:
                update_ids = self.extract_success_ids(update_response)
                result["update_ids"] = update_ids
                print(f"提取的更新IDs: {update_ids}")
            else:
                print("OA更新响应为空")
        
        process_logger.log_end(
            f"OA同步完成",
            table=table_name,
            insert_count=len(result["insert_ids"]),
            update_count=len(result["update_ids"])
        )
        
        print(f"=== OA同步结果汇总 ===")
        print(f"插入成功: {len(result['insert_ids'])}")
        print(f"更新成功: {len(result['update_ids'])}")
        print(f"结果详情: {result}")
        
        return result
    
    async def sync_table_data(self, table_name: str, operation: str = "add") -> Optional[Dict[str, Any]]:
        """同步指定表的数据到OA
        
        Args:
            table_name: 表名
            operation: 操作类型 (add/update)
            
        Returns:
            同步结果
        """
        from database_manager import DatabaseManager
        
        # 获取数据库数据
        db_manager = DatabaseManager()
        
        try:
            # 简单的查询所有数据
            sql = f"SELECT * FROM {table_name} ORDER BY id"
            records = db_manager.execute_query(sql)
            
            if not records:
                process_logger.log_oa_operation("跳过OA同步", reason="无数据需要同步", table=table_name)
                return None
            
            # 构建记录信息
            record_infos = []
            for record in records:
                record_info = {
                    "data": record,
                    "operation": operation,
                    "record_id": record.get("id") or record.get("ID"),
                    "updateid": record.get("updateid") or record.get("UpdateId")
                }
                record_infos.append(record_info)
            
            # 发送到OA
            return await self.send_to_oa(record_infos, table_name, operation)
            
        except Exception as e:
            process_logger.log_error("OA同步", f"查询数据失败: {str(e)}", table=table_name)
            return None
        finally:
            db_manager.close_connection()


class OASyncService:
    """OA同步服务，提供同步业务逻辑"""
    
    def __init__(self):
        self.sync_manager = OASyncManager()
    
    async def sync_contracts_data(self, inserted_records: List[Dict], updated_records: List[Dict]) -> Dict[str, List[str]]:
        """同步合同数据到OA
        
        Args:
            inserted_records: 新插入的合同记录
            updated_records: 更新的合同记录
            
        Returns:
            同步结果
        """
        return await self.sync_manager.sync_records_to_oa(
            inserted_records, 
            updated_records, 
            "contracts"
        )
    
    async def sync_project_funds_data(self, inserted_records: List[Dict], updated_records: List[Dict]) -> Dict[str, List[str]]:
        """同步经费到账数据到OA
        
        Args:
            inserted_records: 新插入的经费记录
            updated_records: 更新的经费记录
            
        Returns:
            同步结果
        """
        return await self.sync_manager.sync_records_to_oa(
            inserted_records, 
            updated_records, 
            "projectfunds"
        )
    
    async def sync_transactions_data(self, inserted_records: List[Dict], updated_records: List[Dict]) -> Dict[str, List[str]]:
        """同步收支明细数据到OA（主从表模式）
        
        Args:
            inserted_records: 新插入的收支明细记录
            updated_records: 更新的收支明细记录
            
        Returns:
            同步结果
        """
        # 创建启用主从表功能的同步管理器
        master_sub_sync_manager = OASyncManager(enable_master_sub_table=True)
        
        return await master_sub_sync_manager.sync_records_to_oa(
            inserted_records, 
            updated_records, 
            "transactions"
        )
    
    def update_local_oa_ids(self, database_manager, sync_results: Dict[str, List[str]],
                           inserted_records: List[Dict], table_name: str):
        """更新本地数据库中的OA同步ID

        Args:
            database_manager: 数据库管理器
            sync_results: OA同步结果
            inserted_records: 插入的记录列表
            table_name: 表名
        """
        insert_ids = sync_results.get("insert_ids", [])

        # 只为新插入的记录更新OA ID
        for i, record_info in enumerate(inserted_records):
            if i < len(insert_ids):
                oa_id = insert_ids[i]

                # 根据表类型确定主键
                if table_name == "contracts":
                    primary_keys = {"contractid": record_info["contractid"]}
                elif table_name == "projectfunds":
                    # 经费表主键为unid（系统生成的UUID）
                    if "unid" in record_info:
                        primary_keys = {"unid": record_info["unid"]}
                    elif "primary_keys" in record_info:
                        primary_keys = record_info["primary_keys"].copy()
                    else:
                        # 从data中获取unid
                        unid = record_info.get("data", {}).get("unid")
                        if unid:
                            primary_keys = {"unid": unid}
                        else:
                            process_logger.log_warning("OA同步", f"经费记录缺少unid，跳过更新OA ID")
                            continue
                elif table_name == "transactions":
                    # 收支明细表的主键字段组合
                    primary_keys = {
                        "fundid": record_info["fundid"],
                        "transactiondate": record_info["transactiondate"],
                        "vouchernumber": record_info["vouchernumber"]
                    }
                else:
                    process_logger.log_warning("OA同步", f"未知表类型: {table_name}")
                    continue

                try:
                    database_manager.update_oa_sync_id(table_name, primary_keys, oa_id)
                except Exception as e:
                    process_logger.log_error("OA同步", f"更新OA同步ID失败: {str(e)}",
                                           table=table_name, oa_id=oa_id)

        process_logger.log_oa_operation(
            "更新本地OA同步ID",
            table=table_name,
            updated_count=min(len(insert_ids), len(inserted_records))
        ) 