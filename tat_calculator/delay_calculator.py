"""
Delay Calculation Module
========================

Calculates stage-level delays by comparing target timestamps with actual timestamps.
Updated to support organized output folder structure.
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from pathlib import Path
import pandas as pd
from .models_config import StagesConfig

logger = logging.getLogger(__name__)


class DelayCalculator:
    """
    Calculates delays for each stage by comparing target vs actual timestamps.
    Now supports organized folder structure for outputs.
    
    Delay = Actual Timestamp - Target Timestamp (positive means delayed)
    """
    
    def __init__(self, config: StagesConfig):
        self.config = config
        # Ensure organized output folders exist
        self._ensure_output_folders()
    
    def _ensure_output_folders(self):
        """Ensure all output folders exist"""
        folders = [
            'outputs/delay_results',
            'outputs/excel_exports',
            'outputs/logs'
        ]
        
        for folder in folders:
            Path(folder).mkdir(parents=True, exist_ok=True)
    
    def calculate_stage_delay(self, stage_id: str, stage_result: Dict[str, Any], po_row: pd.Series) -> Dict[str, Any]:
        """
        Calculate delay for a single stage.
        
        Args:
            stage_id: Stage identifier
            stage_result: Calculated stage result with timestamp and calculation details
            po_row: Original PO data row
            
        Returns:
            Dictionary with delay metrics
        """
        delay_info = {
            "stage_id": stage_id,
            "stage_name": stage_result.get("name"),
            "target_timestamp": None,
            "actual_timestamp": None,
            "delay_days": None,
            "delay_status": "unknown",
            "delay_reason": None,
            "team_responsible": None
        }
        
        # Get target timestamp from stage calculation details (NEW LOGIC)
        target_timestamp = self._extract_target_timestamp(stage_result)
        if target_timestamp:
            delay_info["target_timestamp"] = target_timestamp.isoformat()
        
        # Get actual timestamp from PO data
        stage_config = self.config.stages.get(stage_id)
        if stage_config and stage_config.actual_timestamp:
            actual_value = self._get_actual_timestamp(stage_config.actual_timestamp, po_row)
            if actual_value:
                delay_info["actual_timestamp"] = actual_value.isoformat()
                
                # Calculate delay
                if target_timestamp:
                    delay_days = (actual_value - target_timestamp).days
                    delay_info["delay_days"] = delay_days
                    delay_info["team_responsible"] = stage_config.process_flow.team_owner
                    
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
        
        # If no actual timestamp but target exists
        if target_timestamp and not delay_info["actual_timestamp"]:
            # Check if target date has passed
            if datetime.now() > target_timestamp:
                days_overdue = (datetime.now() - target_timestamp).days
                delay_info["delay_status"] = "pending_overdue"
                delay_info["delay_days"] = days_overdue
                delay_info["delay_reason"] = f"Stage incomplete, {days_overdue} days overdue"
                if stage_config:
                    delay_info["team_responsible"] = stage_config.process_flow.team_owner
            else:
                delay_info["delay_status"] = "pending"
                delay_info["delay_reason"] = "Stage not yet completed"
        
        return delay_info
    
    def _extract_target_timestamp(self, stage_result: Dict[str, Any]) -> Optional[datetime]:
        """
        Extract target timestamp from stage calculation details.
        
        NEW LOGIC: Uses target_date from calculation details instead of timestamp.
        The target_date represents the expected completion time for delay analysis.
        
        Args:
            stage_result: Stage calculation result
            
        Returns:
            Target timestamp as datetime object
        """
        # Try to get target_date from calculation details (NEW APPROACH)
        calculation = stage_result.get("calculation", {})
        if isinstance(calculation, dict):
            target_date = calculation.get("target_date")
            if target_date:
                try:
                    return pd.to_datetime(target_date)
                except Exception as e:
                    logger.warning(f"Failed to parse target_date '{target_date}': {e}")
        
        # Fallback to timestamp for backward compatibility (LEGACY SUPPORT)
        timestamp = stage_result.get("timestamp")
        if timestamp:
            try:
                return pd.to_datetime(timestamp)
            except Exception as e:
                logger.warning(f"Failed to parse timestamp '{timestamp}': {e}")
        
        logger.warning(f"No valid target timestamp found in stage result: {stage_result.get('name', 'Unknown')}")
        return None
    
    def _get_actual_timestamp(self, field_name: str, po_row: pd.Series) -> Optional[datetime]:
        """Extract actual timestamp from PO data"""
        if field_name in po_row.index:
            value = po_row[field_name]
            if pd.notna(value) and value != "" and value != "NA":
                try:
                    return pd.to_datetime(value)
                except:
                    pass
        return None
    
    def calculate_all_delays(self, tat_result: Dict[str, Any], po_row: pd.Series) -> Dict[str, Any]:
        """
        Calculate delays for all stages in a TAT result.
        
        Args:
            tat_result: Complete TAT calculation result
            po_row: Original PO data row
            
        Returns:
            Dictionary with comprehensive delay analysis
        """
        delay_result = {
            "po_id": tat_result.get("po_id"),
            "analysis_date": datetime.now().isoformat(),
            "summary": {
                "total_stages": 0,
                "delayed_stages": 0,
                "early_stages": 0,
                "on_time_stages": 0,
                "pending_stages": 0,
                "pending_overdue_stages": 0,
                "total_delay_days": 0,
                "average_delay_days": 0,
                "critical_path_delays": 0
            },
            "delays_by_team": {},
            "stage_delays": []
        }
        
        stages = tat_result.get("stages", {})
        
        for stage_id, stage_data in stages.items():
            # Calculate delay for this stage
            delay_info = self.calculate_stage_delay(stage_id, stage_data, po_row)
            delay_result["stage_delays"].append(delay_info)
            
            # Update summary statistics
            delay_result["summary"]["total_stages"] += 1
            
            status = delay_info["delay_status"]
            if status == "delayed":
                delay_result["summary"]["delayed_stages"] += 1
                delay_result["summary"]["total_delay_days"] += delay_info["delay_days"]
            elif status == "early":
                delay_result["summary"]["early_stages"] += 1
            elif status == "on_time":
                delay_result["summary"]["on_time_stages"] += 1
            elif status == "pending":
                delay_result["summary"]["pending_stages"] += 1
            elif status == "pending_overdue":
                delay_result["summary"]["pending_overdue_stages"] += 1
                delay_result["summary"]["total_delay_days"] += delay_info["delay_days"]
            
            # Track delays by team
            team = delay_info.get("team_responsible")
            if team and delay_info["delay_days"] is not None:
                if team not in delay_result["delays_by_team"]:
                    delay_result["delays_by_team"][team] = {
                        "total_stages": 0,
                        "delayed_stages": 0,
                        "total_delay_days": 0,
                        "average_delay_days": 0
                    }
                
                team_stats = delay_result["delays_by_team"][team]
                team_stats["total_stages"] += 1
                
                if delay_info["delay_days"] > 0:
                    team_stats["delayed_stages"] += 1
                    team_stats["total_delay_days"] += delay_info["delay_days"]
            
            # Check critical path delays
            stage_config = self.config.stages.get(stage_id)
            if stage_config and stage_config.process_flow.critical_path and status in ["delayed", "pending_overdue"]:
                delay_result["summary"]["critical_path_delays"] += 1
        
        # Calculate averages
        if delay_result["summary"]["delayed_stages"] > 0:
            delay_result["summary"]["average_delay_days"] = round(
                delay_result["summary"]["total_delay_days"] / delay_result["summary"]["delayed_stages"], 2
            )
        
        # Calculate team averages
        for team, stats in delay_result["delays_by_team"].items():
            if stats["delayed_stages"] > 0:
                stats["average_delay_days"] = round(
                    stats["total_delay_days"] / stats["delayed_stages"], 2
                )
        
        # Add insights
        delay_result["insights"] = self._generate_insights(delay_result)
        
        return delay_result
    
    def _generate_insights(self, delay_result: Dict[str, Any]) -> List[str]:
        """Generate actionable insights from delay analysis"""
        insights = []
        
        summary = delay_result["summary"]
        
        # Overall performance
        if summary["delayed_stages"] == 0:
            insights.append("✅ Excellent performance: No delays detected across all stages")
        elif summary["delayed_stages"] / summary["total_stages"] > 0.5:
            insights.append(f"⚠️ Critical: {summary['delayed_stages']} out of {summary['total_stages']} stages are delayed")
        
        # Critical path impact
        if summary["critical_path_delays"] > 0:
            insights.append(f"🚨 {summary['critical_path_delays']} critical path stages are delayed, directly impacting delivery timeline")
        
        # Team-specific insights
        worst_team = None
        worst_delay = 0
        for team, stats in delay_result["delays_by_team"].items():
            if stats["average_delay_days"] > worst_delay:
                worst_delay = stats["average_delay_days"]
                worst_team = team
        
        if worst_team and worst_delay > 5:
            insights.append(f"📊 {worst_team} team has highest average delay of {worst_delay} days")
        
        # Pending overdue
        if summary["pending_overdue_stages"] > 0:
            insights.append(f"⏰ {summary['pending_overdue_stages']} stages are overdue and need immediate attention")
        
        return insights
    
    def export_delay_report(self, delay_results: List[Dict[str, Any]], output_file: str):
        """Export delay analysis to Excel with multiple sheets using organized folder structure"""
        # Ensure the directory exists (in case called with custom path)
        output_path = Path(output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            # Sheet 1: Summary
            summary_data = []
            for result in delay_results:
                summary = result["summary"]
                summary["po_id"] = result["po_id"]
                summary_data.append(summary)
            
            pd.DataFrame(summary_data).to_excel(writer, sheet_name='Summary', index=False)
            
            # Sheet 2: Detailed delays
            all_delays = []
            for result in delay_results:
                for delay in result["stage_delays"]:
                    delay["po_id"] = result["po_id"]
                    all_delays.append(delay)
            
            pd.DataFrame(all_delays).to_excel(writer, sheet_name='Stage Delays', index=False)
            
            # Sheet 3: Team performance
            team_data = []
            for result in delay_results:
                for team, stats in result["delays_by_team"].items():
                    stats["team"] = team
                    stats["po_id"] = result["po_id"]
                    team_data.append(stats)
            
            if team_data:
                pd.DataFrame(team_data).to_excel(writer, sheet_name='Team Performance', index=False)
        
        logger.info(f"Delay report exported to: {output_file}")
    
    def save_delay_analysis_csv(self, delay_results: List[Dict[str, Any]], filename_prefix: str = "delay_analysis") -> str:
        """
        Save delay analysis results to organized CSV folder
        
        Args:
            delay_results: List of delay analysis results
            filename_prefix: Prefix for filename
            
        Returns:
            Full path of saved file
        """
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"outputs/csv_files/{filename_prefix}_{timestamp}.csv"
        
        # Ensure directory exists
        Path(filename).parent.mkdir(parents=True, exist_ok=True)
        
        # Flatten delay results for CSV
        csv_data = []
        for result in delay_results:
            for delay in result["stage_delays"]:
                delay["po_id"] = result["po_id"]
                csv_data.append(delay)
        
        pd.DataFrame(csv_data).to_csv(filename, index=False)
        logger.info(f"Delay analysis CSV saved to: {filename}")
        return filename
