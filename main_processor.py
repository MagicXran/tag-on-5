"""
主处理器模块 - 整合所有组件实现完整的业务流程
"""

import asyncio
import os
from typing import Dict, List, Tuple, Optional, Any
from config import DATABASE_TYPE, get_database_config
from data_cleaner import DataCleaner

# 根据配置选择数据库管理器
if DATABASE_TYPE.lower() == "sqlite":
    from database_manager_sqlite import DatabaseManager
else:
    from database_manager import DatabaseManager

from oa_sync_manager import OASyncService
from logger_utils import process_logger


class RMSTagOnProcessor:
    """RMS标签导入主处理器，负责协调整个数据处理流程"""
    
    def __init__(self):
        self.data_cleaner = DataCleaner()
        self.database_manager = DatabaseManager(get_database_config())
        self.oa_sync_service = OASyncService()
    
    def validate_excel_files(self, folder_path: Optional[str] = None) -> bool:
        """验证指定目录下的Excel文件是否存在
        
        Args:
            folder_path: 目录路径，如果为None则返回False
            
        Returns:
            是否目录存在且包含Excel文件
        """
        if folder_path is None:
            process_logger.log_error("文件验证", "目录路径为空")
            return False
        
        if not os.path.exists(folder_path):
            process_logger.log_error("文件验证", f"目录不存在: {folder_path}")
            return False
        
        # 检查目录下是否有Excel文件
        excel_extensions = ['.xlsx', '.xls']
        excel_files = []
        
        for root, dirs, files in os.walk(folder_path):
            for file in files:
                if any(file.lower().endswith(ext) for ext in excel_extensions):
                    excel_files.append(os.path.join(root, file))
        
        if excel_files:
            process_logger.log_excel_operation("文件验证", folder_path, status=f"存在{len(excel_files)}个Excel文件")
            return True
        else:
            process_logger.log_error("文件验证", f"目录下无Excel文件: {folder_path}")
            return False
    
    def process_contracts_excel(self, file_path: Optional[str] = None) -> Tuple[List[Dict], List[Dict]]:
        """处理合同签订清单Excel文件
        
        Args:
            file_path: Excel文件路径，如果为None则使用配置中的路径
        
        Returns:
            Tuple[List[Dict], List[Dict]]: (插入的记录, 更新的记录)
        """
        process_logger.log_start("处理合同签订清单Excel")
        
        try:
            # 确定要处理的文件路径
            if file_path is None:
                raise ValueError("file_path is None")
            
            # 1. 数据清洗和筛选
            df_contracts = self.data_cleaner.process_contracts_excel(file_path)
            
            if df_contracts.empty:
                process_logger.log_warning("数据处理", "合同清单筛选后无有效数据")
                return [], []
            
            # 2. 保存到数据库
            inserted_records, updated_records = self.database_manager.save_contracts_data(df_contracts)
            
            process_logger.log_end(
                "处理合同签订清单Excel",
                总记录数=len(df_contracts),
                插入记录数=len(inserted_records),
                更新记录数=len(updated_records)
            )
            
            return inserted_records, updated_records
            
        except Exception as e:
            process_logger.log_error("合同Excel处理", str(e))
            raise
    
    def process_project_funds_excel(self, file_path: Optional[str] = None) -> Tuple[List[Dict], List[Dict]]:
        """处理经费到账清单Excel文件
        
        Args:
            file_path: Excel文件路径，如果为None则使用配置中的路径
        
        Returns:
            Tuple[List[Dict], List[Dict]]: (插入的记录, 更新的记录)
        """
        process_logger.log_start("处理经费到账清单Excel")
        
        try:
            # 确定要处理的文件路径
            if file_path is None:
                raise ValueError("file_path is None")
            
            # 1. 数据清洗和筛选
            df_project_funds = self.data_cleaner.process_project_funds_excel(file_path)
            
            if df_project_funds.empty:
                process_logger.log_warning("数据处理", "经费到账清单筛选后无有效数据")
                return [], []
            
            # 2. 保存到数据库
            inserted_records, updated_records = self.database_manager.save_project_funds_data(df_project_funds)
            
            process_logger.log_end(
                "处理经费到账清单Excel",
                总记录数=len(df_project_funds),
                插入记录数=len(inserted_records),
                更新记录数=len(updated_records)
            )
            
            return inserted_records, updated_records
            
        except Exception as e:
            process_logger.log_error("经费Excel处理", str(e))
            raise
    
    async def sync_contracts_to_oa(self, inserted_records: List[Dict], updated_records: List[Dict]) -> Optional[Dict]:
        """异步同步合同数据到OA系统
        
        Args:
            inserted_records: 插入的记录
            updated_records: 更新的记录
            
        Returns:
            同步结果
        """
        if not inserted_records and not updated_records:
            process_logger.log_oa_operation("跳过合同OA同步", reason="无数据需要同步")
            return None
        
        try:
            sync_results = await self.oa_sync_service.sync_contracts_data(
                inserted_records, 
                updated_records
            )
            
            # 更新本地数据库中的OA同步ID
            if sync_results and sync_results.get("insert_ids"):
                self.oa_sync_service.update_local_oa_ids(
                    self.database_manager,
                    sync_results,
                    inserted_records,
                    "contracts"
                )
            
            return sync_results
            
        except Exception as e:
            process_logger.log_error("合同OA同步", str(e))
            return None
    
    async def sync_project_funds_to_oa(self, inserted_records: List[Dict], updated_records: List[Dict]) -> Optional[Dict]:
        """异步同步经费到账数据到OA系统
        
        Args:
            inserted_records: 插入的记录
            updated_records: 更新的记录
            
        Returns:
            同步结果
        """
        if not inserted_records and not updated_records:
            process_logger.log_oa_operation("跳过经费OA同步", reason="无数据需要同步")
            return None
        
        try:
            sync_results = await self.oa_sync_service.sync_project_funds_data(
                inserted_records, 
                updated_records
            )
            
            # 更新本地数据库中的OA同步ID
            if sync_results and sync_results.get("insert_ids"):
                self.oa_sync_service.update_local_oa_ids(
                    self.database_manager,
                    sync_results,
                    inserted_records,
                    "projectfunds"
                )
            
            return sync_results
            
        except Exception as e:
            process_logger.log_error("经费OA同步", str(e))
            return None
    
    async def process_all_data(self, folder_path: Optional[str] = None) -> Dict[str, Dict]:
        """处理所有Excel数据并同步到OA系统
        
        Args:
            folder_path: 用户选择的目录路径，用于验证Excel文件
            
        Returns:
            处理结果摘要
        """
        process_logger.log_start("开始完整数据处理流程")
        
        results = {
            "contracts": {"inserted": 0, "updated": 0, "oa_synced": False},
            "project_funds": {"inserted": 0, "updated": 0, "oa_synced": False},
            "errors": []
        }
        
        try:
            # 1. 验证文件存在性
            if not self.validate_excel_files(folder_path):
                error_msg = "Excel文件验证失败，请检查文件路径"
                process_logger.log_error("文件验证", error_msg)
                results["errors"].append(error_msg)
                return results
            
            # 2. 处理合同签订清单
            try:
                contracts_inserted, contracts_updated = self.process_contracts_excel()
                results["contracts"]["inserted"] = len(contracts_inserted)
                results["contracts"]["updated"] = len(contracts_updated)
                
                # 3. 同步合同数据到OA
                contracts_sync_result = await self.sync_contracts_to_oa(
                    contracts_inserted, 
                    contracts_updated
                )
                results["contracts"]["oa_synced"] = contracts_sync_result is not None
                
            except Exception as e:
                error_msg = f"合同数据处理失败: {str(e)}"
                process_logger.log_error("合同处理", error_msg)
                results["errors"].append(error_msg)
            
            # 4. 处理经费到账清单
            try:
                funds_inserted, funds_updated = self.process_project_funds_excel()
                results["project_funds"]["inserted"] = len(funds_inserted)
                results["project_funds"]["updated"] = len(funds_updated)
                
                # 5. 同步经费数据到OA
                funds_sync_result = await self.sync_project_funds_to_oa(
                    funds_inserted, 
                    funds_updated
                )
                results["project_funds"]["oa_synced"] = funds_sync_result is not None
                
            except Exception as e:
                error_msg = f"经费数据处理失败: {str(e)}"
                process_logger.log_error("经费处理", error_msg)
                results["errors"].append(error_msg)
            
            # 6. 记录处理结果
            process_logger.log_end(
                "完整数据处理流程",
                合同插入=results["contracts"]["inserted"],
                合同更新=results["contracts"]["updated"],
                合同OA同步=results["contracts"]["oa_synced"],
                经费插入=results["project_funds"]["inserted"],
                经费更新=results["project_funds"]["updated"],
                经费OA同步=results["project_funds"]["oa_synced"],
                错误数量=len(results["errors"])
            )
            
        except Exception as e:
            error_msg = f"数据处理流程出现未预期错误: {str(e)}"
            process_logger.log_error("主流程", error_msg)
            results["errors"].append(error_msg)
        
        finally:
            # 确保数据库连接关闭
            self.database_manager.close_connection()
        
        return results
    
    def process_contracts_folder(self, folder_path: Optional[str] = None) -> Dict[str, Any]:
        """
        处理合同文件夹中的所有Excel文件
        
        Args:
            folder_path: 文件夹路径，如果为None则使用配置中的路径
            
        Returns:
            处理结果统计（包含所有插入/更新的记录数据）
        """
        if folder_path is None:
            raise ValueError("folder_path is None")
        
        if not folder_path or not os.path.exists(folder_path):
            raise ValueError(f"合同文件夹不存在: {folder_path}")
        
        process_logger.log_start(f"处理合同文件夹: {folder_path}")
        
        # 获取所有Excel文件
        excel_files = self._get_excel_files(folder_path)
        if not excel_files:
            process_logger.log_warning("文件夹处理", f"文件夹中未找到Excel文件: {folder_path}")
            return {
                "success": False, 
                "message": "未找到Excel文件", 
                "processed_files": 0,
                "all_inserted_records": [],
                "all_updated_records": []
            }
        
        results = {
            "processed_files": 0,
            "total_inserted": 0,
            "total_updated": 0,
            "errors": [],
            "all_inserted_records": [],  # 新增：所有插入的记录
            "all_updated_records": []    # 新增：所有更新的记录
        }
        
        try:
            # 处理每个Excel文件
            for excel_file in excel_files:
                try:
                    process_logger.log_excel_operation("处理合同Excel文件", excel_file)
                    
                    # 直接传递文件路径给处理方法
                    inserted_records, updated_records = self.process_contracts_excel(excel_file)
                    
                    results["processed_files"] += 1
                    results["total_inserted"] += len(inserted_records)
                    results["total_updated"] += len(updated_records)
                    
                    # 收集所有记录数据
                    results["all_inserted_records"].extend(inserted_records)
                    results["all_updated_records"].extend(updated_records)
                    
                    process_logger.log_excel_operation(
                        "合同文件处理完成", 
                        excel_file,
                        插入=len(inserted_records),
                        更新=len(updated_records)
                    )
                    
                except Exception as e:
                    error_msg = f"处理合同文件失败 {excel_file}: {str(e)}"
                    process_logger.log_error("合同文件处理", error_msg)
                    results["errors"].append(error_msg)
            
        except Exception as e:
            error_msg = f"合同文件夹处理失败: {str(e)}"
            process_logger.log_error("合同文件夹处理", error_msg)
            results["errors"].append(error_msg)
        
        process_logger.log_end(
            "合同文件夹处理完成",
            处理文件数=results["processed_files"],
            总插入=results["total_inserted"],
            总更新=results["total_updated"],
            错误数=len(results["errors"])
        )
        
        return results
    
    def process_project_funds_folder(self, folder_path: Optional[str] = None) -> Dict[str, Any]:
        """
        处理经费文件夹中的所有Excel文件
        
        Args:
            folder_path: 文件夹路径，如果为None则使用配置中的路径
            
        Returns:
            处理结果统计（包含所有插入/更新的记录数据）
        """
        if folder_path is None:
            raise ValueError("folder_path is None")
        
        if not folder_path or not os.path.exists(folder_path):
            raise ValueError(f"经费文件夹不存在: {folder_path}")
        
        process_logger.log_start(f"处理经费文件夹: {folder_path}")
        
        # 获取所有Excel文件
        excel_files = self._get_excel_files(folder_path)
        if not excel_files:
            process_logger.log_warning("文件夹处理", f"文件夹中未找到Excel文件: {folder_path}")
            return {
                "success": False, 
                "message": "未找到Excel文件", 
                "processed_files": 0,
                "all_inserted_records": [],
                "all_updated_records": []
            }
        
        results = {
            "processed_files": 0,
            "total_inserted": 0,
            "total_updated": 0,
            "errors": [],
            "all_inserted_records": [],  # 新增：所有插入的记录
            "all_updated_records": []    # 新增：所有更新的记录
        }
        
        try:
            # 处理每个Excel文件
            for excel_file in excel_files:
                try:
                    process_logger.log_excel_operation("处理经费Excel文件", excel_file)
                    
                    # 直接传递文件路径给处理方法
                    inserted_records, updated_records = self.process_project_funds_excel(excel_file)
                    
                    results["processed_files"] += 1
                    results["total_inserted"] += len(inserted_records)
                    results["total_updated"] += len(updated_records)
                    
                    # 收集所有记录数据
                    results["all_inserted_records"].extend(inserted_records)
                    results["all_updated_records"].extend(updated_records)
                    
                    process_logger.log_excel_operation(
                        "经费文件处理完成", 
                        excel_file,
                        插入=len(inserted_records),
                        更新=len(updated_records)
                    )
                    
                except Exception as e:
                    error_msg = f"处理经费文件失败 {excel_file}: {str(e)}"
                    process_logger.log_error("经费文件处理", error_msg)
                    results["errors"].append(error_msg)
            
        except Exception as e:
            error_msg = f"经费文件夹处理失败: {str(e)}"
            process_logger.log_error("经费文件夹处理", error_msg)
            results["errors"].append(error_msg)
        
        process_logger.log_end(
            "经费文件夹处理完成",
            处理文件数=results["processed_files"],
            总插入=results["total_inserted"],
            总更新=results["total_updated"],
            错误数=len(results["errors"])
        )
        
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
    
    def run_sync_process(self, folder_path: Optional[str] = None) -> Dict[str, Dict]:
        """运行同步处理流程（同步版本）
        
        Returns:
            处理结果摘要
        """
        return asyncio.run(self.process_all_data(folder_path))


class ProcessSummaryReporter:
    """处理结果摘要报告器"""
    
    @staticmethod
    def generate_summary_report(results: Dict[str, Dict]) -> str:
        """生成处理结果摘要报告
        
        Args:
            results: 处理结果
            
        Returns:
            摘要报告字符串
        """
        report_lines = [
            "=" * 60,
            "RMS标签导入处理结果摘要",
            "=" * 60,
            ""
        ]
        
        # 合同数据处理结果
        contracts = results.get("contracts", {})
        report_lines.extend([
            f"合同签订清单处理结果：",
            f"  - 新增记录: {contracts.get('inserted', 0)} 条",
            f"  - 更新记录: {contracts.get('updated', 0)} 条",
            f"  - OA同步状态: {'成功' if contracts.get('oa_synced') else '失败或跳过'}",
            ""
        ])
        
        # 经费数据处理结果
        project_funds = results.get("project_funds", {})
        report_lines.extend([
            f"经费到账清单处理结果：",
            f"  - 新增记录: {project_funds.get('inserted', 0)} 条",
            f"  - 更新记录: {project_funds.get('updated', 0)} 条",
            f"  - OA同步状态: {'成功' if project_funds.get('oa_synced') else '失败或跳过'}",
            ""
        ])
        
        # 错误信息
        errors = results.get("errors", [])
        if errors:
            report_lines.extend([
                "处理过程中的错误：",
                ""
            ])
            for i, error in enumerate(errors, 1):
                report_lines.append(f"  {i}. {error}")
            report_lines.append("")
        else:
            report_lines.extend([
                "处理过程无错误发生。",
                ""
            ])
        
        # 总结
        total_inserted = contracts.get('inserted', 0) + project_funds.get('inserted', 0)
        total_updated = contracts.get('updated', 0) + project_funds.get('updated', 0)
        
        report_lines.extend([
            f"总体统计：",
            f"  - 总新增记录: {total_inserted} 条",
            f"  - 总更新记录: {total_updated} 条",
            f"  - 总处理记录: {total_inserted + total_updated} 条",
            f"  - 处理状态: {'成功' if not errors else '部分成功'}",
            ""
        ])
        
        report_lines.append("=" * 60)
        
        return "\n".join(report_lines) 