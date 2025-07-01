"""
TAT Processor Module
====================

Main TAT processing orchestrator with batch processing and export capabilities.
Updated to support organized folder structure and integrated delay information.
"""

import logging
from datetime import datetime
from typing import Dict, List, Any
from pathlib import Path
import pandas as pd
from .models_config import StagesConfig
from .stage_calculator import StageCalculator

logger = logging.getLogger(__name__)


class TATProcessor:
    """
    Orchestrates TAT calculations across all stages and handles output formatting.
    Now supports organized folder structure for outputs and includes delay information.
    """
    
    def __init__(self, config: StagesConfig, stage_calculator: StageCalculator):
        self.config = config
        self.stage_calculator = stage_calculator
        # Ensure organized output folders exist
        self._ensure_output_folders()
    
    def _ensure_output_folders(self):
        """Ensure all output folders exist"""
        folders = [
            'outputs/tat_results',
            'outputs/delay_results', 
            'outputs/excel_exports',
            'outputs/csv_files',
            'outputs/logs'
        ]
        
        for folder in folders:
            Path(folder).mkdir(parents=True, exist_ok=True)
    
    def calculate_tat(self, po_row: pd.Series, include_delays: bool = True) -> Dict[str, Any]:
        """
        Calculate TAT for all stages of a PO with optional delay information
        
        Args:
            po_row: Pandas Series containing PO data
            include_delays: Whether to include delay calculations in results
            
        Returns:
            Dictionary with complete TAT calculation results including delay info
        """
        # Clear cache for new calculation
        self.stage_calculator.reset_cache()
        
        result = {
            "po_id": po_row.get('po_razin_id', 'Unknown'),
            "calculation_date": datetime.now().isoformat(),
            "summary": {
                "total_stages": len(self.config.stages),
                "calculated_stages": 0,
                "methods_used": {
                    "actual_only": 0,
                    "precedence_only": 0,
                    "actual_over_precedence": 0,
                    "precedence_over_actual": 0,
                    "fallback": 0,
                    "failed": 0
                },
                "delay_summary": {
                    "delayed_stages": 0,
                    "early_stages": 0,
                    "on_time_stages": 0,
                    "pending_stages": 0,
                    "pending_overdue_stages": 0,
                    "total_delay_days": 0,
                    "critical_path_delays": 0
                } if include_delays else None
            },
            "stages": {}
        }
        
        # Calculate each stage
        for stage_id, stage_config in self.config.stages.items():
            timestamp, calc_details = self.stage_calculator.calculate_adjusted_timestamp(stage_id, po_row)
            
            # Update summary statistics
            if timestamp:
                result["summary"]["calculated_stages"] += 1
            
            method = calc_details.get("method", "unknown") if isinstance(calc_details, dict) else "legacy"
            if method in result["summary"]["methods_used"]:
                result["summary"]["methods_used"][method] += 1
            
            # Create stage result with calculation info
            stage_result = {
                "name": stage_config.name,
                "timestamp": timestamp.isoformat() if timestamp else None,
                "calculation": self._format_calculation_summary(calc_details, stage_config),
                "process_flow": {
                    "team_owner": stage_config.process_flow.team_owner,
                    "process_type": stage_config.process_flow.process_type,
                    "critical_path": stage_config.process_flow.critical_path,
                    "handoff_points": stage_config.process_flow.handoff_points
                },
                "dependencies": calc_details.get("dependencies", []) if isinstance(calc_details, dict) else []
            }
            
            # Add delay information if requested
            if include_delays:
                delay_info = self._calculate_stage_delay(stage_id, stage_result, po_row)
                stage_result["delay_days"] = delay_info.get("delay_days")
                stage_result["delay_status"] = delay_info.get("delay_status", "unknown")
                stage_result["delay_reason"] = delay_info.get("delay_reason")
                
                # Update delay summary
                self._update_delay_summary(result["summary"]["delay_summary"], delay_info, stage_config)
            
            result["stages"][stage_id] = stage_result
        
        # Calculate completion rate
        result["summary"]["completion_rate"] = round(
            result["summary"]["calculated_stages"] / result["summary"]["total_stages"] * 100, 2
        ) if result["summary"]["total_stages"] > 0 else 0
        
        # Calculate average delay if delays included
        if include_delays and result["summary"]["delay_summary"]["delayed_stages"] > 0:
            result["summary"]["delay_summary"]["average_delay_days"] = round(
                result["summary"]["delay_summary"]["total_delay_days"] / 
                result["summary"]["delay_summary"]["delayed_stages"], 2
            )
        
        return result
    
    def _calculate_stage_delay(self, stage_id: str, stage_result: Dict[str, Any], po_row: pd.Series) -> Dict[str, Any]:
        """
        Calculate delay information for a single stage
        
        Args:
            stage_id: Stage identifier
            stage_result: Calculated stage result with timestamp
            po_row: Original PO data row
            
        Returns:
            Dictionary with delay metrics
        """
        delay_info = {
            "delay_days": None,
            "delay_status": "unknown",
            "delay_reason": None
        }
        
        # Get target timestamp from stage calculation details
        target_timestamp = self._extract_target_timestamp(stage_result)
        
        # Get actual timestamp from PO data
        stage_config = self.config.stages.get(stage_id)
        if stage_config and stage_config.actual_timestamp:
            actual_value = self._get_actual_timestamp(stage_config.actual_timestamp, po_row)
            if actual_value and target_timestamp:
                # Calculate delay
                delay_days = (actual_value - target_timestamp).days
                delay_info["delay_days"] = delay_days
                
                # Categorize delay
                if delay_days > 0:
                    delay_info["delay_status"] = "delayed"
                    delay_info["delay_reason"] = f"Actual completion {delay_days} days after target"
                elif delay_days < 0:
                    delay_info["delay_status"] = "early"
                    delay_info["delay_reason"] = f"Completed {abs(delay_days)} days before target"
                else:
                    delay_info["delay_status"] = "on_time"
                    delay_info["delay_reason"] = "Completed on target date"
            elif target_timestamp and not actual_value:
                # Check if target date has passed
                if datetime.now() > target_timestamp:
                    days_overdue = (datetime.now() - target_timestamp).days
                    delay_info["delay_status"] = "pending_overdue"
                    delay_info["delay_days"] = days_overdue
                    delay_info["delay_reason"] = f"Stage incomplete, {days_overdue} days overdue"
                else:
                    delay_info["delay_status"] = "pending"
                    delay_info["delay_reason"] = "Stage not yet completed"
        
        return delay_info
    
    def _extract_target_timestamp(self, stage_result: Dict[str, Any]):
        """Extract target timestamp from stage calculation details"""
        calculation = stage_result.get("calculation", {})
        if isinstance(calculation, dict):
            target_date = calculation.get("target_date")
            if target_date:
                try:
                    return pd.to_datetime(target_date)
                except Exception:
                    pass
        
        # Fallback to timestamp
        timestamp = stage_result.get("timestamp")
        if timestamp:
            try:
                return pd.to_datetime(timestamp)
            except Exception:
                pass
        
        return None
    
    def _get_actual_timestamp(self, field_name: str, po_row: pd.Series):
        """Extract actual timestamp from PO data"""
        if field_name in po_row.index:
            value = po_row[field_name]
            if pd.notna(value) and value != "" and value != "NA":
                try:
                    return pd.to_datetime(value)
                except:
                    pass
        return None
    
    def _update_delay_summary(self, delay_summary: Dict[str, Any], delay_info: Dict[str, Any], stage_config):
        """Update delay summary statistics"""
        status = delay_info["delay_status"]
        
        if status == "delayed":
            delay_summary["delayed_stages"] += 1
            if delay_info["delay_days"]:
                delay_summary["total_delay_days"] += delay_info["delay_days"]
        elif status == "early":
            delay_summary["early_stages"] += 1
        elif status == "on_time":
            delay_summary["on_time_stages"] += 1
        elif status == "pending":
            delay_summary["pending_stages"] += 1
        elif status == "pending_overdue":
            delay_summary["pending_overdue_stages"] += 1
            if delay_info["delay_days"]:
                delay_summary["total_delay_days"] += delay_info["delay_days"]
        
        # Check critical path delays
        if stage_config.process_flow.critical_path and status in ["delayed", "pending_overdue"]:
            delay_summary["critical_path_delays"] += 1
    
    def _format_calculation_summary(self, calc_details: Dict[str, Any], stage_config) -> Dict[str, Any]:
        """
        Format calculation details into a clean, readable summary
        
        Args:
            calc_details: Raw calculation details
            stage_config: Stage configuration
            
        Returns:
            Clean calculation summary
        """
        if not isinstance(calc_details, dict):
            return {"method": "legacy", "summary": str(calc_details)}
        
        method = calc_details.get("method", "unknown")
        
        summary = {
            "method": method,
            "source": calc_details.get("source"),
            "decision": calc_details.get("decision_reason"),
            "lead_time_days": calc_details.get("lead_time_applied", 0),
            "target_date": calc_details.get("target_date")
        }
        
        # Add method-specific details
        if method == "actual_over_precedence":
            summary.update({
                "actual_date": calc_details.get("actual_value"),
                "precedence_date": calc_details.get("precedence_value"),
                "reason": "Actual timestamp is later than calculated precedence"
            })
        elif method == "precedence_over_actual":
            summary.update({
                "actual_date": calc_details.get("actual_value"),
                "precedence_date": calc_details.get("precedence_value"), 
                "reason": "Calculated precedence is later than actual timestamp"
            })
        elif method == "precedence_only":
            summary.update({
                "precedence_date": calc_details.get("precedence_date"),
                "reason": "No actual timestamp available"
            })
        elif method == "actual_only":
            summary.update({
                "actual_field": calc_details.get("actual_field"),
                "reason": "No dependency chain available"
            })
        elif method == "fallback":
            summary.update({
                "target_date": calc_details.get("target_date"),
                "expression": calc_details.get("source"),
                "reason": "Using fallback calculation"
            })
        
        return summary
    
    def process_batch(self, df: pd.DataFrame, include_delays: bool = True) -> List[Dict[str, Any]]:
        """
        Process multiple POs in batch
        
        Args:
            df: DataFrame containing multiple PO rows
            include_delays: Whether to include delay calculations
            
        Returns:
            List of TAT calculation results with delay information
        """
        results = []
        
        for index, row in df.iterrows():
            try:
                result = self.calculate_tat(row, include_delays=include_delays)
                results.append(result)
                logger.info(f"Processed PO: {result['po_id']}")
            except Exception as e:
                logger.error(f"Error processing row {index}: {e}")
                results.append({
                    "po_id": row.get('po_razin_id', f'Row_{index}'),
                    "error": str(e),
                    "calculation_date": datetime.now().isoformat()
                })
        
        return results

    def export_to_excel(self, df: pd.DataFrame, results: List[Dict[str, Any]], output_file: str):
        """
        Export original data + calculated timestamps + delay info to Excel
        
        Args:
            df: Original DataFrame
            results: TAT calculation results
            output_file: Output Excel file path (should include organized folder path)
        """
        # Ensure the directory exists (in case called with custom path)
        output_path = Path(output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Create a copy of the original dataframe
        export_df = df.copy()
        
        # Add calculated timestamps for each stage
        for result in results:
            if 'stages' not in result:
                continue
                
            po_id = result['po_id']
            po_index = export_df[export_df['po_razin_id'] == po_id].index
            
            if len(po_index) > 0:
                idx = po_index[0]
                
                # Add calculated timestamps and delay info
                for stage_id, stage_data in result['stages'].items():
                    # Use stage name instead of ID for column name
                    stage_name = stage_data['name']
                    
                    # Add timestamp column
                    col_name = f"{stage_name}_Date"
                    timestamp = stage_data['timestamp']
                    if timestamp:
                        date = pd.to_datetime(timestamp).date()
                        export_df.loc[idx, col_name] = date
                    else:
                        export_df.loc[idx, col_name] = None
                    
                    # Add delay columns if available
                    if 'delay_days' in stage_data:
                        delay_col = f"{stage_name}_Delay_Days"
                        status_col = f"{stage_name}_Status"
                        export_df.loc[idx, delay_col] = stage_data['delay_days']
                        export_df.loc[idx, status_col] = stage_data['delay_status']
        
        # Save to Excel
        export_df.to_excel(output_file, index=False)
        logger.info(f"Results exported to: {output_file}")
    
    def export_stage_level_excel(self, df: pd.DataFrame, results: List[Dict[str, Any]], output_file: str):
        """
        Export stage-level data to Excel with 3 separate tabs:
        - actual_timestamps: Actual timestamps from PO data
        - timestamps: Calculated timestamps from TAT processing
        - delay_days: Delay days for each stage
        
        Args:
            df: Original DataFrame
            results: TAT calculation results
            output_file: Output Excel file path
        """
        # Ensure the directory exists
        output_path = Path(output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Get all stage configurations
        stage_configs = {stage_id: config for stage_id, config in self.config.stages.items()}
        
        # Prepare data for each tab
        po_ids = []
        actual_timestamps_data = {}
        calculated_timestamps_data = {}
        delay_days_data = {}
        
        # Initialize stage columns
        for stage_id, stage_config in stage_configs.items():
            stage_name = stage_config.name
            actual_timestamps_data[stage_name] = []
            calculated_timestamps_data[stage_name] = []
            delay_days_data[stage_name] = []
        
        # Process each result
        for result in results:
            if 'stages' not in result:
                continue
                
            po_id = result['po_id']
            po_ids.append(po_id)
            
            # Get original PO row
            po_row = df[df['po_razin_id'] == po_id]
            if len(po_row) == 0:
                po_row = pd.Series()
            else:
                po_row = po_row.iloc[0]
            
            # Process each stage
            for stage_id, stage_config in stage_configs.items():
                stage_name = stage_config.name
                stage_result = result['stages'].get(stage_id, {})
                
                # 1. Actual timestamps (from PO data)
                actual_timestamp = None
                if stage_config.actual_timestamp and len(po_row) > 0:
                    actual_value = self._get_actual_timestamp(stage_config.actual_timestamp, po_row)
                    if actual_value:
                        actual_timestamp = actual_value.date()
                actual_timestamps_data[stage_name].append(actual_timestamp)
                
                # 2. Calculated timestamps (from TAT processing)
                calculated_timestamp = None
                if stage_result.get('timestamp'):
                    calculated_timestamp = pd.to_datetime(stage_result['timestamp']).date()
                calculated_timestamps_data[stage_name].append(calculated_timestamp)
                
                # 3. Delay days
                delay_days = stage_result.get('delay_days')
                delay_days_data[stage_name].append(delay_days)
        
        # Create DataFrames for each tab
        actual_df = pd.DataFrame({'PO_ID': po_ids, **actual_timestamps_data})
        calculated_df = pd.DataFrame({'PO_ID': po_ids, **calculated_timestamps_data})
        delay_df = pd.DataFrame({'PO_ID': po_ids, **delay_days_data})
        
        # Write to Excel with multiple tabs
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            actual_df.to_excel(writer, sheet_name='actual_timestamps', index=False)
            calculated_df.to_excel(writer, sheet_name='timestamps', index=False)
            delay_df.to_excel(writer, sheet_name='delay_days', index=False)
        
        logger.info(f"Stage-level results exported to: {output_file}")
        logger.info(f"  - actual_timestamps tab: {len(po_ids)} POs x {len(stage_configs)} stages")
        logger.info(f"  - timestamps tab: {len(po_ids)} POs x {len(stage_configs)} stages")  
        logger.info(f"  - delay_days tab: {len(po_ids)} POs x {len(stage_configs)} stages")
    
    def save_to_csv(self, df: pd.DataFrame, filename_prefix: str = "processed_data") -> str:
        """
        Save processed DataFrame to organized CSV folder
        
        Args:
            df: DataFrame to save
            filename_prefix: Prefix for filename
            
        Returns:
            Full path of saved file
        """
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"outputs/csv_files/{filename_prefix}_{timestamp}.csv"
        
        # Ensure directory exists
        Path(filename).parent.mkdir(parents=True, exist_ok=True)
        
        df.to_csv(filename, index=False)
        logger.info(f"CSV saved to: {filename}")
        return filename
