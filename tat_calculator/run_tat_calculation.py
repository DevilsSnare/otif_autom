"""
Complete TAT Calculation Runner
==============================

This script runs the complete TAT calculation system on your Excel data
and generates comprehensive reports and analytics with integrated delay information.
"""

import pandas as pd
import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
import sys
import traceback
import os

# Set up organized folder structure
def setup_output_folders():
    """Create organized output folder structure"""
    folders = [
        'outputs/tat_results',
        'outputs/delay_results', 
        'outputs/excel_exports',
        'outputs/csv_files',
        'outputs/logs'
    ]
    
    for folder in folders:
        Path(folder).mkdir(parents=True, exist_ok=True)
    
    return folders

# Create folders before setting up logging
setup_output_folders()

# Set up comprehensive logging with organized paths
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('outputs/logs/tat_calculation.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class TATRunner:
    """Complete TAT calculation runner with enhanced reporting, organized outputs, and integrated delays"""
    
    def __init__(self, excel_file: str = "ts_big.xlsx", config_file: str = "stages_config.json", dataframe: pd.DataFrame = None):
        base_dir = os.path.dirname(os.path.abspath(__file__))
        self.config_file = os.path.join(base_dir, config_file) 

        self.excel_file = excel_file
        self.config_file = config_file
        self.df = None
        self.calculator = None
        self.results = []
        self.delay_results = []
        self.df = dataframe
        
        # Ensure output folders exist
        self.output_folders = setup_output_folders()
        logger.info(f"Output folders created: {self.output_folders}")
        
    def setup(self):
        """Set up the calculator and load data"""
        logger.info("Setting up TAT calculation environment...")
        
        # Load Excel data
        # self.load_excel_data()
        if self.df is None:
            self.load_excel_data()
        else:
            logger.info("Using provided dataframe")
            self.df.columns = self.df.columns.str.strip()
            self.convert_date_columns()
            self.validate_required_columns()
        
        # Initialize calculator
        self.initialize_calculator()
        
        logger.info("Setup completed successfully!")
        
    def load_excel_data(self):
        """Load and prepare Excel data"""
        logger.info(f"Loading Excel file: {self.excel_file}")
        
        try:
            self.df = pd.read_excel(self.excel_file)
            logger.info(f"Loaded {len(self.df)} rows and {len(self.df.columns)} columns")
            
            # Clean column names
            self.df.columns = self.df.columns.str.strip()
            
            # Convert date columns
            self.convert_date_columns()
            
            # Validate required columns
            self.validate_required_columns()
            
        except Exception as e:
            logger.error(f"Error loading Excel file: {e}")
            raise
            
    def convert_date_columns(self):
        """Convert relevant columns to datetime"""
        date_columns = [
            'po_created_date', 'po_approval_date', 'supplier_confirmation_date',
            'pi_invoice_approval_date', 'pi_payment_date', 'receive_first_prd_date',
            'prd_reconfirmed_date', 'po_im_date_value', 'po_sm_date_value',
            'batch_created_ts', 'sm_signoff_ts', 'ci_invoice_approval_date',
            'ci_payment_date', 'qc_schedule_date', 'ffw_booking_ts', 'spd_ts',
            'stock_pickup_date', 'shipment_creation_date', 'shipment_in_transit_date',
            'bi_invoice_approval_date', 'bi_payment_date', 'ffwp_telex_release_date',
            'shipment_stock_delivery_date', 'item_receipt_date', 'actual_cargo_pick_up_date',
            'actual_shipping_date', 'actual_arrival_date', 'actual_delivery_date'
        ]
        
        for col in date_columns:
            if col in self.df.columns:
                original_type = self.df[col].dtype
                self.df[col] = pd.to_datetime(self.df[col], errors='coerce')
                self.df[col] = self.df[col].astype(object).where(pd.notna(self.df[col]), None)

                missing_count = self.df[col].isna().sum()
                total_count = len(self.df)
                
                logger.info(f"Converted {col}: {original_type} -> datetime, "
                          f"{missing_count}/{total_count} missing ({missing_count/total_count*100:.1f}%)")
    
    def validate_required_columns(self):
        """Validate that required columns exist"""
        required_columns = ['po_razin_id', 'po_created_date']
        missing_columns = [col for col in required_columns if col not in self.df.columns]
        
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")
            
        logger.info("All required columns present")
    
    def initialize_calculator(self):
        """Initialize the TAT calculator"""
        logger.info(f"Initializing TAT Calculator with config: {self.config_file}")
        
        try:
            # Import the new modular TATCalculator
            from .tat_calculator_main import TATCalculator
            self.calculator = TATCalculator(self.config_file)
            logger.info("TAT Calculator initialized successfully")
            
        except ImportError:
            # Fallback to old monolithic version if new modules not available
            logger.warning("New modular TATCalculator not available, trying old version...")
            try:
                from .tat_calculator import TATCalculator
                self.calculator = TATCalculator(self.config_file)
                logger.info("TAT Calculator (old version) initialized successfully")
            except ImportError:
                logger.error("No TATCalculator available. Please ensure tat_calculator_main.py or tat_calculator.py is in the same directory.")
                raise
        except Exception as e:
            logger.error(f"Error initializing calculator: {e}")
            raise
    
    def run_calculations(self, sample_size: int = None, include_detailed_delays: bool = True):
        """Run TAT calculations with integrated delay information"""
        if sample_size:
            df_to_process = self.df.head(sample_size)
            logger.info(f"Processing sample of {sample_size} POs")
        else:
            df_to_process = self.df
            logger.info(f"Processing all {len(df_to_process)} POs")
        
        self.results = []
        self.delay_results = []
        errors = []
        
        # Check if calculator supports new integrated delay functionality
        has_integrated_delays = hasattr(self.calculator, 'process_batch_with_delays')
        
        if has_integrated_delays and include_detailed_delays:
            logger.info("Using integrated delay calculation...")
            try:
                # New method: get both TAT results with delays and detailed delay analysis
                self.results, self.delay_results = self.calculator.process_batch_with_delays(df_to_process)
                logger.info(f"Completed calculations with integrated delays: {len(self.results)} TAT results, {len(self.delay_results)} delay analyses")
                return self.results
            except Exception as e:
                logger.warning(f"Error with integrated delay calculation, falling back to individual processing: {e}")
                has_integrated_delays = False
        
        # Fallback to individual processing
        for index, row in df_to_process.iterrows():
            try:
                po_id = row.get('po_razin_id', f'Row_{index}')
                logger.info(f"Processing PO {index + 1}/{len(df_to_process)}: {po_id}")
                
                # Calculate TAT with integrated delays
                if hasattr(self.calculator.calculate_tat, '__code__') and 'include_delays' in self.calculator.calculate_tat.__code__.co_varnames:
                    result = self.calculator.calculate_tat(row, include_delays=True)
                else:
                    result = self.calculator.calculate_tat(row)
                
                self.results.append(result)
                
                # Calculate detailed delays if requested and method exists
                if include_detailed_delays and hasattr(self.calculator, 'calculate_delay'):
                    delay_result = self.calculator.calculate_delay(result, row)
                    self.delay_results.append(delay_result)
                
            except Exception as e:
                error_info = {
                    'index': index,
                    'po_id': row.get('po_razin_id', f'Row_{index}'),
                    'error': str(e),
                    'traceback': traceback.format_exc()
                }
                errors.append(error_info)
                logger.error(f"Error processing row {index}: {e}")
        
        logger.info(f"Completed calculations: {len(self.results)} successful, {len(errors)} errors")
        
        if errors:
            self.save_errors(errors)
        
        return self.results
    
    def save_errors(self, errors):
        """Save error details to organized logs folder"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        error_file = f"outputs/logs/tat_errors_{timestamp}.json"
        
        with open(error_file, 'w') as f:
            json.dump(errors, f, indent=2)
        logger.info(f"Error details saved to: {error_file}")
    
    def save_results(self, filename_prefix: str = "tat_results_with_delays"):
        """Save TAT calculation results with integrated delay information"""
        if not self.results:
            logger.warning("No results to save")
            return
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"outputs/tat_results/{filename_prefix}_{timestamp}.json"
        
        with open(filename, 'w') as f:
            json.dump(self.results, f, indent=2, default=str)
        
        logger.info(f"TAT results with delays saved to: {filename}")
        return filename
    
    def save_delay_results(self, filename_prefix: str = "detailed_delay_analysis"):
        """Save detailed delay analysis results"""
        if not self.delay_results:
            logger.warning("No detailed delay results to save")
            return
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"outputs/delay_results/{filename_prefix}_{timestamp}.json"
        
        with open(filename, 'w') as f:
            json.dump(self.delay_results, f, indent=2, default=str)
        
        logger.info(f"Detailed delay results saved to: {filename}")
        return filename
    
    def export_to_excel(self, filename_prefix: str = "tat_export_with_delays"):
        """Export original data + calculated timestamps + delay info to Excel"""
        if not self.results:
            logger.warning("No results to export")
            return
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"outputs/excel_exports/{filename_prefix}_{timestamp}.xlsx"
        
        # Use the organized path in the export method
        self.calculator.export_to_excel(self.df, self.results, filename)
        return filename
    
    def export_stage_level_excel(self, filename_prefix: str = "stage_level_analysis"):
        """Export stage-level data to Excel with 3 tabs: actual_timestamps, timestamps, delay_days"""
        if not self.results:
            logger.warning("No results to export")
            return
        
        # Check if calculator supports stage-level export
        if not hasattr(self.calculator, 'export_stage_level_excel'):
            logger.warning("Stage-level Excel export not available in current calculator version")
            return
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"outputs/excel_exports/{filename_prefix}_{timestamp}.xlsx"
        
        # Use the new stage-level export method
        self.calculator.export_stage_level_excel(self.df, self.results, filename)
        return filename
    
    def export_delay_report(self, filename_prefix: str = "detailed_delay_report"):
        """Export detailed delay analysis report to Excel"""
        if not self.delay_results:
            logger.warning("No detailed delay results to export")
            return
        
        if not hasattr(self.calculator, 'export_delay_report'):
            logger.warning("Detailed delay report export not available in current calculator version")
            return
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"outputs/excel_exports/{filename_prefix}_{timestamp}.xlsx"
        
        # Use the organized path in the export method
        self.calculator.export_delay_report(self.delay_results, filename)
        return filename
    
    def save_processed_csv(self, filename_prefix: str = "processed_data"):
        """Save processed CSV data to organized csv_files folder"""
        if self.df is None:
            logger.warning("No data to save as CSV")
            return
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"outputs/csv_files/{filename_prefix}_{timestamp}.csv"
        
        self.df.to_csv(filename, index=False)
        logger.info(f"Processed CSV saved to: {filename}")
        return filename
    
    def print_delay_summary(self):
        """Print a summary of delay information from TAT results"""
        if not self.results:
            print("No results available for delay summary")
            return
        
        print("\n📊 Delay Summary from TAT Results:")
        print("=" * 50)
        
        total_delayed = 0
        total_early = 0
        total_on_time = 0
        total_pending_overdue = 0
        max_delay = 0
        worst_po = None
        
        for result in self.results:
            if 'stages' not in result:
                continue
                
            po_delayed = 0
            for stage_id, stage_data in result['stages'].items():
                delay_status = stage_data.get('delay_status')
                delay_days = stage_data.get('delay_days', 0)
                
                if delay_status == 'delayed':
                    total_delayed += 1
                    po_delayed += delay_days or 0
                elif delay_status == 'early':
                    total_early += 1
                elif delay_status == 'on_time':
                    total_on_time += 1
                elif delay_status == 'pending_overdue':
                    total_pending_overdue += 1
                
                if delay_days and delay_days > max_delay:
                    max_delay = delay_days
                    worst_po = f"{result['po_id']} - Stage {stage_data['name']}"
            
            # Check delay summary if available
            delay_summary = result.get('summary', {}).get('delay_summary')
            if delay_summary:
                print(f"PO {result['po_id']}: {delay_summary.get('delayed_stages', 0)} delayed stages, "
                      f"{delay_summary.get('total_delay_days', 0)} total delay days")
        
        print(f"\n🎯 Overall Statistics:")
        print(f"   Delayed stages: {total_delayed}")
        print(f"   Early stages: {total_early}")
        print(f"   On-time stages: {total_on_time}")
        print(f"   Overdue stages: {total_pending_overdue}")
        print(f"   Worst delay: {max_delay} days ({worst_po})")


def main(final_df):
    """Main execution function"""
    print("TAT Calculation System with Stage-Level Analysis - Starting...")
    print("=" * 70)
    
    try:
        # Initialize runner
        runner = TATRunner(dataframe=final_df)
        
        # Setup
        runner.setup()
        
        # Run calculations (start with sample for testing)
        sample_size = 5  # Process first 5 POs for testing
        print(f"\nRunning calculations on sample of {sample_size} POs...")
        results = runner.run_calculations(sample_size=sample_size, include_detailed_delays=True)
        
        if results:
            # Print delay summary
            runner.print_delay_summary()
            
            # Save results to organized folders
            results_file = runner.save_results()
            delay_results_file = runner.save_delay_results()
            processed_csv_file = runner.save_processed_csv()
            
            # Export to Excel in organized folders
            excel_file = runner.export_to_excel()
            stage_level_file = runner.export_stage_level_excel()
            delay_report_file = runner.export_delay_report()
            
            print(f"\n Organized Output Structure:")
            print(f"├── outputs/")
            print(f"    ├── tat_results/")
            print(f"    │   └── {os.path.basename(results_file) if results_file else 'No TAT results'}")
            print(f"    │       (includes delay_days & delay_status for each stage)")
            print(f"    ├── delay_results/")
            print(f"    │   └── {os.path.basename(delay_results_file) if delay_results_file else 'No detailed delay results'}")
            print(f"    │       (detailed delay analysis with insights)")
            print(f"    ├── excel_exports/")
            print(f"    │   ├── {os.path.basename(excel_file) if excel_file else 'No Excel export'}")
            print(f"    │   │   (includes delay columns: Stage_Delay_Days, Stage_Status)")
            print(f"    │   ├── {os.path.basename(stage_level_file) if stage_level_file else 'No stage-level export'}")
            print(f"    │   │    (3 tabs: actual_timestamps, timestamps, delay_days)")
            print(f"    │   └── {os.path.basename(delay_report_file) if delay_report_file else 'No delay report'}")
            print(f"    ├── csv_files/")
            print(f"    │   └── {os.path.basename(processed_csv_file) if processed_csv_file else 'No CSV file'}")
            print(f"    └── logs/")
            print(f"        └── tat_calculation.log")
            
            print(f"\n New Feature: Stage-Level Excel Export!")
            print(f"     actual_timestamps tab: Actual timestamps from PO data")
            print(f"      timestamps tab: Calculated timestamps from TAT processing")
            print(f"     delay_days tab: Delay days for each stage")
            print(f"   Format: PO_ID vs All Stages in matrix layout")
            
            # Show example of stage-level output structure
            if stage_level_file:
                print(f"\nStage-Level Excel Structure:")
                print(f"   Tab 1: actual_timestamps")
                print(f"   ┌─────────────┬──────────────────┬─────────────────┬───")
                print(f"   │ PO_ID       │ 01. PO Approval  │ 02. Supplier... │ ...")
                print(f"   ├─────────────┼──────────────────┼─────────────────┼───")
                print(f"   │ PO123...    │ 2025-06-01       │ 2025-06-03      │ ...")
                print(f"   └─────────────┴──────────────────┴─────────────────┴───")
                print(f"   ")
                print(f"   Tab 2: timestamps (calculated)")
                print(f"   Tab 3: delay_days (difference analysis)")
            
        print(f"\nTo process all POs, change sample_size=None in the run_calculations() call")
        print("TAT Calculation with stage-level analysis completed successfully!")
        
    except Exception as e:
        logger.error(f"Error in main execution: {e}")
        print(f"Error: {e}")
        print(" See outputs/logs/tat_calculation.log for detailed error information")

    return stage_level_file

# if __name__ == "__main__":
#     main()
