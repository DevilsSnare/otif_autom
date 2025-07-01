"""
TAT Calculator Main Module
=========================

Main TATCalculator class that coordinates all sub-modules.
Updated to include integrated delay information in TAT results.
"""
import os
import logging
from typing import List, Dict, Any
import pandas as pd
from .models_config import load_config, validate_config
from .expression_evaluator import ExpressionEvaluator
from .stage_calculator import StageCalculator
from .tat_processor import TATProcessor
from .delay_calculator import DelayCalculator

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TATCalculator:
    """
    Core TAT calculation engine that processes PO data through configurable stages.
    
    This is the main entry point that coordinates all sub-modules:
    - Configuration management (models_config)
    - Expression evaluation (expression_evaluator)
    - Stage calculations (stage_calculator)
    - TAT processing and export (tat_processor)
    - Delay analysis (delay_calculator)
    
    Updated to include delay information directly in TAT results.
    """
    
    def __init__(self, config_path: str = "stages_config.json"):
        """
        Initialize the TAT Calculator
        
        Args:
            config_path: Path to the stages configuration JSON file
        """
        base_dir = os.path.dirname(os.path.abspath(__file__))
        config_path = os.path.join(base_dir, config_path)
        
        # Load and validate configuration
        self.config = load_config(config_path)
        validate_config(self.config)
        
        # Initialize sub-components
        self.expression_evaluator = ExpressionEvaluator()
        self.stage_calculator = StageCalculator(self.config, self.expression_evaluator)
        self.tat_processor = TATProcessor(self.config, self.stage_calculator)
        self.delay_calculator = DelayCalculator(self.config)
        
        logger.info(f"TAT Calculator initialized with {len(self.config.stages)} stages")
    
    def calculate_tat(self, po_row: pd.Series, include_delays: bool = True) -> Dict[str, Any]:
        """
        Calculate TAT for all stages of a PO with integrated delay information
        
        Args:
            po_row: Pandas Series containing PO data
            include_delays: Whether to include delay calculations in results (default: True)
            
        Returns:
            Dictionary with complete TAT calculation results including delay info
        """
        return self.tat_processor.calculate_tat(po_row, include_delays=include_delays)
    
    def calculate_delay(self, tat_result: Dict[str, Any], po_row: pd.Series) -> Dict[str, Any]:
        """
        Calculate delays for all stages based on TAT results (separate analysis)
        
        Args:
            tat_result: TAT calculation result
            po_row: Original PO data row
            
        Returns:
            Dictionary with detailed delay analysis
        """
        return self.delay_calculator.calculate_all_delays(tat_result, po_row)
    
    def process_batch(self, df: pd.DataFrame, include_delays: bool = True) -> List[Dict[str, Any]]:
        """
        Process multiple POs in batch with integrated delay information
        
        Args:
            df: DataFrame containing multiple PO rows
            include_delays: Whether to include delay calculations (default: True)
            
        Returns:
            List of TAT calculation results with delay information
        """
        return self.tat_processor.process_batch(df, include_delays=include_delays)
    
    def process_batch_with_delays(self, df: pd.DataFrame) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """
        Process multiple POs and calculate detailed delays (separate analysis)
        
        Args:
            df: DataFrame containing multiple PO rows
            
        Returns:
            Tuple of (TAT results with integrated delays, Detailed delay analysis)
        """
        # Get TAT results with integrated delay info
        tat_results = self.process_batch(df, include_delays=True)
        
        # Generate detailed delay analysis
        delay_results = []
        for i, tat_result in enumerate(tat_results):
            if 'error' not in tat_result:
                po_row = df.iloc[i]
                delay_result = self.calculate_delay(tat_result, po_row)
                delay_results.append(delay_result)
        
        return tat_results, delay_results
    
    def export_to_excel(self, df: pd.DataFrame, results: List[Dict[str, Any]], output_file: str):
        """
        Export original data + calculated timestamps + delay info to Excel
        
        Args:
            df: Original DataFrame
            results: TAT calculation results (with integrated delay info)
            output_file: Output Excel file path
        """
        self.tat_processor.export_to_excel(df, results, output_file)
    
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
        self.tat_processor.export_stage_level_excel(df, results, output_file)
    
    def export_delay_report(self, delay_results: List[Dict[str, Any]], output_file: str):
        """
        Export detailed delay analysis report to Excel
        
        Args:
            delay_results: List of detailed delay analysis results
            output_file: Output Excel file path
        """
        self.delay_calculator.export_delay_report(delay_results, output_file)
    
    # Convenience methods for backward compatibility
    def _get_date_value(self, field_name: str, po_row: pd.Series):
        """Legacy method for backward compatibility"""
        return self.expression_evaluator.get_date_value(field_name, po_row)
    
    def _evaluate_expression(self, expression: str, po_row: pd.Series):
        """Legacy method for backward compatibility"""
        return self.expression_evaluator.evaluate_expression(expression, po_row)
    
    def calculate_adjusted_timestamp(self, stage_id: str, po_row: pd.Series):
        """Legacy method for backward compatibility"""
        return self.stage_calculator.calculate_adjusted_timestamp(stage_id, po_row)


if __name__ == "__main__":
    print("TAT Calculator System - Enhanced with Integrated Delay Information")
    print("=" * 65)
    print("Key Features:")
    print("✅ Integrated delay info in TAT results (delay_days, delay_status)")
    print("✅ Organized output folder structure")
    print("✅ Comprehensive delay analysis")
    print("✅ Excel export with delay columns")
    print("✅ Stage-level Excel export with 3 tabs")
    print()
    print("Usage Examples:")
    print("1. Basic TAT calculation with delays:")
    print("   calculator = TATCalculator()")
    print("   result = calculator.calculate_tat(po_row)")
    print("   # result['stages']['8']['delay_days'] = 5")
    print("   # result['stages']['8']['delay_status'] = 'delayed'")
    print()
    print("2. Batch processing:")
    print("   results = calculator.process_batch(df)")
    print("   calculator.export_to_excel(df, results, 'outputs/excel_exports/tat_with_delays.xlsx')")
    print()
    print("3. Stage-level Excel export:")
    print("   calculator.export_stage_level_excel(df, results, 'outputs/excel_exports/stage_level.xlsx')")
    print("   # Creates 3 tabs: actual_timestamps, timestamps, delay_days")
    print()
    print("4. Detailed delay analysis:")
    print("   tat_results, delay_results = calculator.process_batch_with_delays(df)")
    print("   calculator.export_delay_report(delay_results, 'outputs/excel_exports/delay_analysis.xlsx')")
