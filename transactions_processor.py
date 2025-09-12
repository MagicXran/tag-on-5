"""
收支明细表处理器
负责从指定文件夹读取Excel收支明细表，进行数据清理和保存
"""

import os
import re
import pandas as pd
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Any
from datetime import datetime

from config import (
    EXCEL_FILES, MYSQL_CONFIG, OA_CONFIG, RUNTIME_CONFIG,
    TRANSACTIONS_FIELD_TYPES, TRANSACTIONS_COLUMN_MAPPING,
    TRANSACTIONS_EXCEL_CONSTANTS, TYPE_CONVERTERS,TRANSACTIONS_PRIMARY_KEYS,
    DATABASE_TYPE, get_database_config
)

# 根据配置选择数据库管理器
if DATABASE_TYPE.lower() == "sqlite":
    from database_manager_sqlite import DatabaseManager
else:
    from database_manager import DatabaseManager
from oa_sync_manager import OASyncManager
from logger_utils import LoggerManager, process_logger


class TransactionsProcessor:
    """收支明细表处理器"""
    
    def __init__(self):
        self.logger = LoggerManager.get_logger("TransactionsProcessor")
        self.db_manager = DatabaseManager(get_database_config())
        
        if RUNTIME_CONFIG["enable_oa_sync"]:
            self.oa_manager = OASyncManager(
                OA_CONFIG,
                enable_master_sub_table=RUNTIME_CONFIG["enable_oa_master_sub_table"]
            )
        else:
            self.oa_manager = None
        
        # Excel解析常量
        self.excel_constants = TRANSACTIONS_EXCEL_CONSTANTS
        
    def process_transactions_folder(self, folder_path: str = None) -> Dict[str, Any]:
        """
        处理收支明细表文件夹中的所有Excel文件
        
        Args:
            folder_path: 文件夹路径，如果为None则使用配置中的路径
            
        Returns:
            处理结果统计
        """
        if folder_path is None:
            folder_path = EXCEL_FILES.get("transactions")
        
        if not folder_path or not os.path.exists(folder_path):
            raise ValueError(f"收支明细表文件夹不存在: {folder_path}")
        
        self.logger.info(f"开始处理收支明细表文件夹: {folder_path}")
        
        # 获取所有Excel文件
        excel_files = self._get_excel_files(folder_path)
        if not excel_files:
            self.logger.warning(f"文件夹中未找到Excel文件: {folder_path}")
            return {"success": False, "message": "未找到Excel文件"}
        
        results = {
            "processed_files": 0,
            "total_records": 0,
            "successful_records": 0,
            "failed_records": 0,
            "errors": []
        }
        
        # 处理每个Excel文件
        for excel_file in excel_files:
            try:
                self.logger.info(f"处理Excel文件: {excel_file}")
                file_result = self._process_single_excel(excel_file)
                
                results["processed_files"] += 1
                results["total_records"] += file_result["total_records"]
                results["successful_records"] += file_result["successful_records"]
                results["failed_records"] += file_result["failed_records"]
                
                if file_result["errors"]:
                    results["errors"].extend(file_result["errors"])
                    
            except Exception as e:
                error_msg = f"处理文件失败 {excel_file}: {str(e)}"
                self.logger.error(error_msg)
                results["errors"].append(error_msg)
        
        self.logger.info(f"文件夹处理完成，共处理{results['processed_files']}个文件，"
                        f"成功{results['successful_records']}条，失败{results['failed_records']}条")
        
        return results
    
    def _get_excel_files(self, folder_path: str) -> List[str]:
        """获取文件夹中所有Excel文件"""
        excel_extensions = ['.xlsx', '.xls']
        excel_files = []
        
        for root, dirs, files in os.walk(folder_path):
            for file in files:
                if any(file.lower().endswith(ext) for ext in excel_extensions):
                    excel_files.append(os.path.join(root, file))
        
        return excel_files
    
    def _process_single_excel(self, excel_file_path: str) -> Dict[str, Any]:
        """
        处理单个Excel文件
        
        Args:
            excel_file_path: Excel文件路径
            
        Returns:
            处理结果
        """
        try:
            # 1. 读取Excel文件
            workbook = pd.ExcelFile(excel_file_path)
            sheet_name = workbook.sheet_names[0]  # 使用第一个工作表
            df = pd.read_excel(excel_file_path, sheet_name=sheet_name, header=None)
            
            # 2. 解析经费卡号信息
            fund_info = self._parse_fund_header(df)
            if not fund_info:
                raise ValueError("无法解析经费卡号信息")
            
            # 3. 解析收支明细数据
            transactions_data = self._parse_transactions_data(df, fund_info)
            
            # 4. 解析汇总信息
            summary_info = self._parse_summary_data(df)
            
            # 5. 合并数据
            final_data = self._merge_data(fund_info, transactions_data, summary_info)
            
            # 6. 数据类型转换和清理
            cleaned_data = self._clean_data(final_data)
            
            # 7. 保存到数据库
            db_result = self._save_to_database(cleaned_data)
            
            # 8. (已移除无意义的projectfunds表更新操作)
            
            # 9. 同步到OA系统 - 传递原始数据（中文字段名）而不是清理后的数据
            oa_result = None
            if self.oa_manager and final_data:
                import asyncio
                oa_result = asyncio.run(self._sync_to_oa(final_data, db_result))
            
            return {
                "total_records": len(final_data),
                "successful_records": len(cleaned_data),
                "failed_records": len(final_data) - len(cleaned_data),
                "errors": [],
                "db_result": db_result,
                "oa_result": oa_result
            }
            
        except Exception as e:
            self.logger.error(f"处理Excel文件失败 {excel_file_path}: {str(e)}")
            raise
    
    def _parse_fund_header(self, df: pd.DataFrame) -> Optional[Dict[str, str]]:
        """
        解析经费卡号信息（第2行C2-H2合并单元格）
        
        格式示例: 39320284（有预算）钛合金薄带轧制成形关键技术及产 起始日期：2025-01-01 终止日期：2025-03-26
        """
        try:
            # 读取第2行C列到H列的内容
            header_row = self.excel_constants["HEADER_ROW"] - 1  # 转为0-based索引
            start_col = self.excel_constants["HEADER_COLUMN_START"]
            end_col = self.excel_constants["HEADER_COLUMN_END"]
            
            # 合并单元格内容
            header_content = ""
            for col in range(start_col, end_col + 1):
                if col < len(df.columns) and header_row < len(df):
                    cell_value = df.iloc[header_row, col]
                    if pd.notna(cell_value) and str(cell_value).strip():
                        header_content += str(cell_value).strip()
                        break  # 合并单元格只需要第一个有值的单元格
            
            if not header_content:
                self.logger.error("未找到经费卡号信息")
                return None
            
            self.logger.debug(f"解析经费卡号信息: {header_content}")
            
            # 解析经费卡号
            fund_id_pattern = r"^(\d+)"
            fund_id_match = re.match(fund_id_pattern, header_content)
            if not fund_id_match:
                self.logger.error(f"无法解析经费卡号: {header_content}")
                return None
            
            fund_id = fund_id_match.group(1)
            
            # 移除经费卡号部分
            remaining_content = header_content[len(fund_id):].strip()
            
            # 移除括号部分（如果存在）
            bracket_pattern = r"^[（(][^）)]*[）)]"
            remaining_content = re.sub(bracket_pattern, "", remaining_content).strip()
            
            # 解析起始日期和终止日期
            start_date = ""
            end_date = ""
            project_name = remaining_content
            
            date_patterns = [
                r"起始日期[：:]\s*(\d{4}-\d{2}-\d{2})",
                r"终止日期[：:]\s*(\d{4}-\d{2}-\d{2})"
            ]
            
            for pattern in date_patterns:
                match = re.search(pattern, remaining_content)
                if match:
                    if "起始" in pattern:
                        start_date = match.group(1)
                    else:
                        end_date = match.group(1)
                    # 从项目名称中移除日期部分
                    project_name = project_name.replace(match.group(0), "").strip()
            
            return {
                "经费卡号": fund_id,
                "项目名称": project_name,
                "起始日期": start_date,
                "终止日期": end_date
            }
            
        except Exception as e:
            self.logger.error(f"解析经费卡号信息失败: {str(e)}")
            return None
    
    def _parse_transactions_data(self, df: pd.DataFrame, fund_info: Dict[str, str]) -> List[Dict[str, Any]]:
        """解析收支明细数据"""
        try:
            data_start_row = self.excel_constants["DATA_START_ROW"] - 1  # 转为0-based索引
            field_names = self.excel_constants["FIELD_NAMES"]
            
            transactions = []
            sequence_number = 1  # 序号1字段
            
            for idx in range(data_start_row, len(df)):
                # 检查是否是汇总行
                row_data = df.iloc[idx]
                if self._is_summary_row(row_data):
                    break
                
                # 检查是否有有效数据
                if self._is_empty_row(row_data):
                    continue
                
                # 构建记录
                record = {
                    "经费卡号": fund_info["经费卡号"],
                    "项目名称": fund_info["项目名称"],
                    "序号1": sequence_number
                }
                
                # 映射字段数据
                for col_idx, field_name in enumerate(field_names):
                    if col_idx < len(row_data):
                        record[field_name] = row_data.iloc[col_idx]
                
                transactions.append(record)
                sequence_number += 1
            
            self.logger.info(f"解析到{len(transactions)}条收支明细记录")
            return transactions
            
        except Exception as e:
            self.logger.error(f"解析收支明细数据失败: {str(e)}")
            return []
    
    def _parse_summary_data(self, df: pd.DataFrame) -> Dict[str, float]:
        """解析汇总数据（累计发生额和期末余额）"""
        try:
            summary_data = {
                "借方累计发生额": 0.0,
                "贷方累计发生额": 0.0,
                "期末余额": 0.0
            }
            
            for idx in range(len(df)):
                row_data = df.iloc[idx]
                
                # 检查摘要列（C列，索引2）
                if len(row_data) > 2:
                    summary_desc = str(row_data.iloc[2]).strip()
                    
                    if "累计发生额" in summary_desc:
                        # 读取F列（借方金额）和G列（贷方金额）
                        if len(row_data) > 5:  # F列索引5
                            debit_val = self._safe_float_convert(row_data.iloc[5])
                            summary_data["借方累计发生额"] = debit_val
                        
                        if len(row_data) > 6:  # G列索引6
                            credit_val = self._safe_float_convert(row_data.iloc[6])
                            summary_data["贷方累计发生额"] = credit_val
                    
                    elif "期末余额" in summary_desc:
                        # 读取H列（余额）
                        if len(row_data) > 7:  # H列索引7
                            balance_val = self._safe_float_convert(row_data.iloc[7])
                            summary_data["期末余额"] = balance_val
            
            self.logger.info(f"解析汇总数据: {summary_data}")
            return summary_data
            
        except Exception as e:
            self.logger.error(f"解析汇总数据失败: {str(e)}")
            return {"借方累计发生额": 0.0, "贷方累计发生额": 0.0, "期末余额": 0.0}
    
    def _merge_data(self, fund_info: Dict, transactions: List[Dict], summary: Dict) -> List[Dict]:
        """合并基础信息、明细数据和汇总信息"""
        merged_data = []
        
        for transaction in transactions:
            record = transaction.copy()
            # 添加汇总信息到每条记录
            record.update(summary)
            merged_data.append(record)
        
        return merged_data
    
    def _clean_data(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """数据清理和类型转换 - 严格按照config配置进行"""
        cleaned_data = []
        
        for record in data:
            cleaned_record = {}
            
            for field_name, field_value in record.items():
                # 获取字段类型
                field_type = TRANSACTIONS_FIELD_TYPES.get(field_name, "str")
                
                # 对于经费卡号字段，特殊处理避免小数点问题
                if field_name == "经费卡号" and field_type == "str":
                    cleaned_value = self._clean_fund_id_field(field_value)
                else:
                    # 使用配置的类型转换器
                    converter = TYPE_CONVERTERS.get(field_type, TYPE_CONVERTERS["str"])
                    try:
                        cleaned_value = converter(field_value)
                    except Exception as e:
                        self.logger.warning(f"字段{field_name}类型转换失败: {field_value} -> {field_type}, 错误: {e}")
                        # 使用默认值
                        if field_type == "str":
                            cleaned_value = ""
                        elif field_type == "float":
                            cleaned_value = 0.0
                        elif field_type == "int":
                            cleaned_value = 0
                        else:
                            cleaned_value = ""
                
                cleaned_record[field_name] = cleaned_value
            
            cleaned_data.append(cleaned_record)
        
        return cleaned_data
    
    def _clean_fund_id_field(self, fund_id: Any) -> str:
        """清洗经费卡号字段，保持原始格式不变
        
        Args:
            fund_id: 经费卡号原始值
            
        Returns:
            str: 清洗后的经费卡号字符串
        """
        if pd.isna(fund_id) or not fund_id:
            return ""
        
        # 直接转换为字符串，保持原始格式
        return str(fund_id).strip()
    
    def _save_to_database(self, data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """保存数据到数据库"""
        if not data:
            return {"success": True, "inserted": 0, "updated": 0}
        
        try:
            # 转换为数据库列名
            db_records = []
            for record in data:
                db_record = {}
                for ch_name, value in record.items():
                    en_name = TRANSACTIONS_COLUMN_MAPPING.get(ch_name, ch_name)
                    db_record[en_name] = value
                db_records.append(db_record)
            
            # 批量保存到数据库 - 使用正确的联合主键
            result = self.db_manager.upsert_records(
                "transactions", 
                db_records, 
                key_fields=TRANSACTIONS_PRIMARY_KEYS  # 修正：只使用数据库中定义的唯一约束字段
            )
            
            self.logger.info(f"数据库保存完成: 插入{result.get('inserted', 0)}条，更新{result.get('updated', 0)}条")
            return result
            
        except Exception as e:
            self.logger.error(f"保存数据到数据库失败: {str(e)}")
            raise
    

    async def _sync_to_oa(self, data: List[Dict[str, Any]], db_result: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """同步数据到OA系统 - 基于new_main.py逻辑重构"""
        try:
            if not self.oa_manager:
                self.logger.warning("OA管理器未初始化，跳过OA同步")
                return None
            
            # 检查是否有数据需要同步
            if not data:
                self.logger.info("无数据需要同步到OA")
                return None
            
            # 根据数据库操作结果决定OA操作类型
            operation = "add" if db_result.get("inserted", 0) > 0 else "update"
            
            # 如果是更新操作，需要先查询updateid
            if operation == "update":
                data = await self._enrich_data_with_updateid(data)
            
            # 构建OA请求数据，基于new_main.py的主从表逻辑
            request_payload = self._build_oa_request_payload(data, operation)
            
            # 发送请求到OA
            oa_result = await self._send_oa_request(request_payload, operation)
            
            # 处理OA响应，保存updateid到数据库
            if oa_result and operation == "add":
                success_ids = self._extract_success_ids(oa_result)
                if success_ids:
                    await self._save_update_ids(data, success_ids, request_payload)
            
            self.logger.info(f"OA同步完成: {oa_result}")
            return oa_result
            
        except Exception as e:
            self.logger.error(f"OA同步失败: {str(e)}")
            return None
    
    async def _enrich_data_with_updateid(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """为更新操作的数据查询并添加updateid"""
        enriched_data = []
        
        for record in data:
            enriched_record = record.copy()
            
            # 从数据库查询updateid
            try:
                query = """
                    SELECT updateid FROM transactions 
                    WHERE fundid = ? AND transactiondate = ? AND vouchernumber = ? AND balance = ?
                    AND updateid IS NOT NULL AND updateid != ''
                """
                
                params = (
                    record.get("经费卡号"),
                    record.get("日期"),
                    record.get("凭证号"), 
                    record.get("余额")
                )
                
                result = self.db_manager.execute_query(query, params)
                if result and len(result) > 0:
                    updateid = result[0]["updateid"]
                    enriched_record["updateid"] = updateid
                    self.logger.debug(f"找到updateid: {updateid} for 经费卡号={record.get('经费卡号')}, 凭证号={record.get('凭证号')}")
                else:
                    self.logger.warning(f"未找到updateid for 经费卡号={record.get('经费卡号')}, 凭证号={record.get('凭证号')}")
                    
            except Exception as e:
                self.logger.error(f"查询updateid失败: {str(e)}")
            
            enriched_data.append(enriched_record)
        
        return enriched_data
    
    def _build_oa_request_payload(self, data: List[Dict[str, Any]], operation: str = "add") -> Dict:
        """构建OA请求载荷 - 基于new_main.py的主从表逻辑"""
        table_name = "transactions"
        table_info = {
            "masterTable": "formmain_0018",
            "subTable": "formson_0019", 
            "rightId": "-5159805959137505715.-6609998234064302859",
            "formCode": "shouzhimingxi",
            "groupKey": "fundid",
            # 只排除在从表中不需要的主表字段
            "excludeFields": ["fundid", "transname"]
        }
        
        # 主表字段映射：中文名 -> OA字段编码
        master_field_mapping = {
            "经费卡号": "field0001",
            "项目名称": "field0002",
            "借方累计发生额": "field0003",
            "贷方累计发生额": "field0004",
            "期末余额": "field0005",
            "项目编号": "field0015"  # 如果数据中有的话
        }
        
        # 从表字段映射：中文名 -> OA字段编码
        sub_field_mapping = {
            "序号1": "field0006",
            "日期": "field0007",
            "凭证号": "field0008",
            "摘要": "field0009",
            "科目代码": "field0010",
            "科目名称": "field0011",
            "借方金额": "field0012",
            "贷方金额": "field0013",
            "余额": "field0014"
        }
        
        # 按经费卡号分组数据（主从表逻辑）
        groups = {}
        for record in data:
            fund_id = record.get("经费卡号")
            if fund_id:
                if fund_id not in groups:
                    groups[fund_id] = []
                groups[fund_id].append(record)
        
        data_list = []
        for fund_id, records in groups.items():
            # 构建主表记录 - 包含主表字段
            master_record = records[0].copy()  # 取第一条记录作为主表数据源
            
            # 查询并添加项目编号（合同号）
            try:
                query = "SELECT contractid FROM projectfunds WHERE fundid = ? AND contractid IS NOT NULL AND contractid != ''"
                result = self.db_manager.execute_query(query, (fund_id,))
                if result and len(result) > 0:
                    master_record["项目编号"] = result[0]["contractid"]
                    self.logger.debug(f"为fundid={fund_id}查询到项目编号: {result[0]['contractid']}")
                else:
                    master_record["项目编号"] = ""
                    self.logger.warning(f"未找到fundid={fund_id}对应的项目编号")
            except Exception as e:
                self.logger.error(f"查询项目编号失败: {str(e)}")
                master_record["项目编号"] = ""
            
            master_oa_record = self._build_oa_record(
                master_record, master_field_mapping, operation, 
                exclude_fields=[], exclude_main_fields=False
            )
            
            # 构建子表记录 - 包含所有明细记录的从表字段
            sub_oa_records = []
            for sub_record in records:  # 每条明细记录都作为子表记录
                sub_oa_record = self._build_oa_record(
                    sub_record, sub_field_mapping, operation, 
                    exclude_fields=[], exclude_main_fields=False
                )
                sub_oa_records.append(sub_oa_record)
            
            entry = {
                "masterTable": {
                    "name": table_info["masterTable"],
                    "record": master_oa_record,
                    "changedFields": []
                },
                "subTables": []
            }
            
            # 添加子表记录
            if sub_oa_records:
                entry["subTables"].append({
                    "name": table_info["subTable"],
                    "records": sub_oa_records,
                    "changedFields": []
                })
            
            data_list.append(entry)
        
        payload = {
            "formCode": table_info["formCode"],
            "loginName": self.oa_manager.login_name,
            "rightId": table_info["rightId"],
            "doTrigger": "false",
            "dataList": data_list
        }
        
        return payload
    
    def _build_oa_record(self, record: Dict, field_mapping: Dict, operation: str, 
                        exclude_fields: List[str] = None, exclude_main_fields: bool = True) -> Dict:
        """构建单条OA记录"""
        exclude_fields = exclude_fields or []
        
        fields = []
        for ch_name, value in record.items():
            # 跳过排除的字段
            if exclude_main_fields and ch_name in exclude_fields:
                continue
                
            if ch_name in field_mapping:
                oa_field = field_mapping[ch_name]
                
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
                    "showValue": ch_name
                })
        
        oa_record = {
            "fields": fields
        }
        
        # 设置记录ID - 基于new_main.py逻辑
        if operation == "update" and record.get("updateid"):
            oa_record["id"] = record["updateid"]
        else:
            # 对于新记录，使用序号或生成ID
            oa_record["id"] = record.get("序号1", 1)
        
        return oa_record
    
    async def _send_oa_request(self, payload: Dict, operation: str) -> Optional[Dict]:
        """发送OA请求"""
        try:
            # 获取令牌
            token = await self.oa_manager.get_token()
            if not token:
                self.logger.error("无法获取OA访问令牌")
                return None
            
            # 构建请求
            url = f"{self.oa_manager.base_url}/seeyon/rest/cap4/form/soap/batch-{operation}"
            headers = {
                "token": token,
                "Content-Type": "application/json"
            }
            
            # 添加调试日志：打印发送给OA的JSON字符串
            import json
            payload_json = json.dumps(payload, ensure_ascii=False, indent=2)
            self.logger.info(f"发送给OA的JSON载荷:\n{payload_json}")
            
            process_logger.log_oa_operation(
                f"发送{operation}请求",
                table="transactions",
                record_count=len(payload["dataList"]),
                url=url
            )
            
            # 发送异步请求
            import aiohttp
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload, headers=headers, timeout=30) as response:
                    if response.status == 200:
                        result = await response.json()
                        
                        # 添加调试日志：打印OA返回的JSON字符串
                        result_json = json.dumps(result, ensure_ascii=False, indent=2)
                        self.logger.info(f"OA返回的JSON响应:\n{result_json}")
                        
                        process_logger.log_oa_operation(
                            f"{operation}响应",
                            status="成功",
                            table="transactions",
                            response_keys=list(result.keys())
                        )
                        return result
                    else:
                        text = await response.text()
                        self.logger.error(f"OA请求失败: HTTP {response.status}, {text[:200]}")
                        return None
        except Exception as e:
            self.logger.error(f"发送OA请求失败: {str(e)}")
            return None
    
    def _extract_success_ids(self, oa_response: Dict) -> List[str]:
        """从OA响应中提取成功的ID列表 - 基于new_main.py逻辑"""
        try:
            data = oa_response.get("data")
            if data:
                success_ids = data.get("successIdList", [])
                if success_ids:
                    self.logger.info(f"OA返回{len(success_ids)}个成功ID")
                    return success_ids
                else:
                    self.logger.warning("OA响应中无successIdList")
            else:
                # 检查是否是简单的成功响应
                if oa_response.get("success") is True:
                    self.logger.info("OA操作成功，但无返回ID")
                else:
                    self.logger.warning(f"OA响应格式异常: {oa_response}")
            return []
        except Exception as e:
            self.logger.error(f"提取OA成功ID失败: {str(e)}")
            return []
    
    async def _save_update_ids(self, original_data: List[Dict], success_ids: List[str], 
                              request_payload: Dict) -> None:
        """将OA返回的ID保存到数据库updateid字段 - 基于new_main.py逻辑"""
        try:
            if not success_ids:
                return
            
            # 根据经费卡号分组，与请求载荷的dataList顺序对应
            fund_groups = {}
            for record in original_data:
                fund_id = record.get("经费卡号")
                if fund_id:
                    if fund_id not in fund_groups:
                        fund_groups[fund_id] = []
                    fund_groups[fund_id].append(record)
            
            # 按照请求载荷中的顺序更新updateid
            data_list = request_payload.get("dataList", [])
            for idx, (fund_id, success_id) in enumerate(zip(fund_groups.keys(), success_ids)):
                if idx < len(data_list):
                    # 更新该经费卡号下所有记录的updateid
                    update_sql = """
                        UPDATE transactions 
                        SET updateid = ? 
                        WHERE fundid = ?
                    """
                    
                    self.db_manager.execute_update(update_sql, (success_id, fund_id))
                    self.logger.info(f"更新fundid={fund_id}的updateid为{success_id}")
                    
        except Exception as e:
            self.logger.error(f"保存updateid到数据库失败: {str(e)}")
    
    def _is_summary_row(self, row_data: pd.Series) -> bool:
        """判断是否是汇总行"""
        if len(row_data) > 2:
            summary_desc = str(row_data.iloc[2]).strip()
            return any(keyword in summary_desc for keyword in self.excel_constants["SUMMARY_KEYWORDS"])
        return False
    
    def _is_empty_row(self, row_data: pd.Series) -> bool:
        """判断是否是空行"""
        return row_data.isna().all() or all(str(val).strip() == "" for val in row_data if pd.notna(val))
    
    def _safe_float_convert(self, value) -> float:
        """安全的浮点数转换"""
        if value is None or (isinstance(value, float) and str(value).lower() == 'nan'):
            return 0.0
        try:
            if isinstance(value, str):
                value = value.replace(',', '').strip()
            return float(value)
        except (ValueError, TypeError):
            return 0.0


def main():
    """主程序入口"""
    processor = TransactionsProcessor()
    
    try:
        # 处理收支明细表文件夹
        results = processor.process_transactions_folder()
        
        print(f"处理完成:")
        print(f"- 处理文件数: {results['processed_files']}")
        print(f"- 总记录数: {results['total_records']}")
        print(f"- 成功记录数: {results['successful_records']}")
        print(f"- 失败记录数: {results['failed_records']}")
        
        if results['errors']:
            print(f"- 错误信息: {results['errors']}")
        
    except Exception as e:
        print(f"程序执行失败: {str(e)}")


if __name__ == "__main__":
    main() 