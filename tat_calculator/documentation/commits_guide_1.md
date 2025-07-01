# Commits Guide - TAT Calculator Project

## Overview
This document tracks significant changes and provides guidance for commit messages in the TAT Calculator project.

## Commit Message Format
```
[Type] Brief description of change

- Detailed bullet point 1
- Detailed bullet point 2
- Impact or reason for change
```

## Change Types
- **Fix**: Bug fixes and corrections
- **Feature**: New functionality
- **Refactor**: Code restructuring without behavior change
- **Config**: Changes to stages_config.json
- **Docs**: Documentation updates
- **Performance**: Performance improvements

## Recent Changes Log

### 2025-06-26: Integrate delay information directly into TAT results
**Type**: Feature  
**Files Modified**: `tat_processor.py`, `tat_calculator_main.py`, `run_tat_calculation.py`  
**Commit SHA**: 401d5d4442bd861519f0335bbe64752c15ce1466

**Issue**: Users wanted delay information (`delay_days` and `delay_status`) included directly in TAT results JSON structure, not just in separate delay analysis files. This would provide a comprehensive view of each stage's performance in a single result.

**Solution**: 
- **Enhanced TAT Processor**: Modified `calculate_tat()` to include delay calculations by default
- **Integrated Delay Fields**: Added `delay_days`, `delay_status`, and `delay_reason` to each stage result
- **Delay Summary**: Added comprehensive delay summary to result statistics
- **Excel Export Enhancement**: Updated Excel export to include delay columns for each stage
- **Backward Compatibility**: Maintained `include_delays` parameter for optional delay integration

**Impact**: 
- **Single Source of Truth**: All TAT and delay information in one JSON result
- **Enhanced Analytics**: Rich delay summary statistics in TAT results
- **Better Excel Reports**: Delay columns automatically included in Excel exports
- **Improved User Experience**: No need to cross-reference separate delay files
- **Comprehensive Insights**: Stage-level delay information readily available

**Code Changes**:

#### New TAT Results Structure:
```json
{
  "po_id": "PO374147PROB-0001921",
  "summary": {
    "completion_rate": 90.32,
    "delay_summary": {
      "delayed_stages": 3,
      "early_stages": 1,
      "on_time_stages": 2,
      "total_delay_days": 15,
      "average_delay_days": 5.0,
      "critical_path_delays": 1
    }
  },
  "stages": {
    "8": {
      "name": "08. PRD Pending",
      "timestamp": "2025-06-13T00:00:00",
      "delay_days": 5,
      "delay_status": "delayed",
      "delay_reason": "Actual completion 5 days after target",
      "calculation": { ... },
      "process_flow": { ... }
    }
  }
}
```

#### Enhanced Excel Export:
- **Before**: Only timestamp columns (`Stage_Name_Date`)
- **After**: Timestamp + delay columns (`Stage_Name_Date`, `Stage_Name_Delay_Days`, `Stage_Name_Status`)

#### Backward Compatibility:
```python
# New default behavior (delays included)
result = calculator.calculate_tat(po_row)

# Explicit delay inclusion
result = calculator.calculate_tat(po_row, include_delays=True)

# Disable delays if needed
result = calculator.calculate_tat(po_row, include_delays=False)
```

**User Benefits**:
- ✅ **Unified Results**: All information in single TAT result
- ✅ **Rich Analytics**: Comprehensive delay statistics
- ✅ **Better Reports**: Enhanced Excel exports with delay data
- ✅ **Faster Analysis**: No need to merge separate files
- ✅ **Stage Insights**: Immediate visibility into stage performance

### 2025-06-26: Implement organized folder structure for clean file outputs
**Type**: Feature  
**Files Modified**: `run_tat_calculation.py`, `tat_processor.py`, `delay_calculator.py`  
**Commit SHA**: cdfd4e5845b4c6f5abb82d89ebd62f13971f5fc0

**Issue**: All output files (TAT results, delay reports, Excel exports, CSVs, logs) were being saved to the root directory, creating a cluttered and disorganized file structure.

**Solution**: 
- Implemented organized folder structure with segregated directories:
  - `outputs/tat_results/` - TAT calculation JSON results
  - `outputs/delay_results/` - Delay analysis JSON results  
  - `outputs/excel_exports/` - Excel files (TAT exports, delay reports)
  - `outputs/csv_files/` - Processed CSV data files
  - `outputs/logs/` - Log files and error reports
- Added automatic folder creation utilities
- Updated all save/export methods to use organized paths
- Enhanced visual output display showing folder structure
- Maintained backward compatibility for existing code

**Impact**: 
- Clean, organized project structure
- Easy file navigation and management
- Better separation of different output types
- Professional project organization
- Scalable structure for future additions

### 2025-06-26: Fix delay calculation to use target_date from stage_calculations
**Type**: Fix  
**Files Modified**: `delay_calculator.py`  
**Commit SHA**: 8750a230e2fb6ebe2b38a0f8b78c9eb8b08f1dcf

**Issue**: Delay calculations were incorrectly using `stage_result["timestamp"]` as the target timestamp, which represents the final calculated completion time rather than the target date for delay analysis.

**Solution**: 
- Modified `calculate_stage_delay()` to extract `target_date` from stage calculation details
- Changed from using `stage_result["timestamp"]` to `stage_result["calculation"]["target_date"]`
- Added robust target timestamp extraction logic with fallback support
- Maintained backward compatibility for different calculation result structures
- Enhanced error handling and logging for missing target_date fields

**Impact**: 
- Delay calculations now use the correct baseline (target_date) for comparison
- More accurate delay analysis and reporting
- Better alignment between TAT calculations and delay analysis
- Improved transparency in delay calculation methodology

## Development Guidelines

### Before Making Changes
1. Review the project_brain.md for architecture understanding
2. Check existing code patterns and naming conventions
3. Ensure changes align with modular structure principles
4. Consider impact on file organization and outputs
5. **Consider delay integration implications for new features**

### Change Process
1. Make targeted, specific changes
2. Test changes on sample data
3. Update relevant documentation
4. Ensure organized folder structure is maintained
5. **Verify delay information is properly integrated where applicable**
6. Commit with descriptive message following the format above
7. Update this commits_guide.md with change details

### Configuration Changes
- Most business logic changes should be in `stages_config.json`
- Code changes should only be for new features or bug fixes
- Always validate configuration after changes

### Testing Approach
- Test with `ts_small.xlsx` first
- Verify calculations match expected results
- Check JSON output structure remains consistent
- **Verify delay information is correctly integrated in TAT results**
- Ensure Excel export functionality works with delay columns
- Verify organized folder structure is created and used correctly

### Output Organization Best Practices
- Always use organized folder structure for new outputs
- Maintain consistent naming conventions with timestamps
- Ensure folder creation before file operations
- Provide clear feedback on where files are saved
- Keep file types properly segregated
- **Include delay information in relevant outputs**

## Future Improvements Tracking

### Planned Enhancements
- [ ] Enhanced delay reporting with trend analysis
- [ ] Performance optimization for large datasets
- [ ] Additional custom functions in expression evaluator
- [ ] Automated testing framework
- [ ] Archive functionality for old outputs
- [ ] Configuration-driven output folder customization
- [ ] Real-time delay alerts and notifications
- [ ] Interactive delay dashboard

### Known Issues
- None currently identified

## Folder Structure Guidelines

### Standard Output Folders
```
outputs/
├── tat_results/        # TAT calculation JSON results (with integrated delays)
├── delay_results/      # Detailed delay analysis JSON results
├── excel_exports/      # Excel files (exports with delay columns, reports)
├── csv_files/          # Processed CSV data
└── logs/              # Application logs and errors
```

### File Naming Conventions
- **Format**: `{prefix}_{YYYYMMDD_HHMMSS}.{ext}`
- **TAT Results**: `tat_results_with_delays_20250626_100352.json`
- **Delay Results**: `detailed_delay_analysis_20250626_100352.json`
- **Excel Exports**: `tat_export_with_delays_20250626_100352.xlsx`
- **CSV Files**: `processed_data_20250626_100352.csv`
- **Logs**: `tat_calculation.log` (single log file with rotation)

## Delay Integration Guidelines

### TAT Results with Delays
Each stage now includes:
```json
{
  "delay_days": 5,           // Number of delay days (positive = late, negative = early)
  "delay_status": "delayed", // Status: delayed, early, on_time, pending, pending_overdue
  "delay_reason": "Actual completion 5 days after target"
}
```

### Summary Statistics
TAT results include delay summary:
```json
{
  "delay_summary": {
    "delayed_stages": 3,
    "early_stages": 1,
    "on_time_stages": 2,
    "total_delay_days": 15,
    "average_delay_days": 5.0,
    "critical_path_delays": 1
  }
}
```

### Excel Export Enhancements
For each stage, Excel now includes:
- `{Stage_Name}_Date` - Calculated timestamp
- `{Stage_Name}_Delay_Days` - Delay in days
- `{Stage_Name}_Status` - Delay status

---

**Note**: This guide should be updated after every significant change to maintain project history and development patterns.


# TAT Calculator Logic Alignment - Fix Summary

## Issue Identified
After commit `f185c4974185bdf01487f0bc911e63116b6d6956`, subsequent commits introduced a modular structure that inadvertently changed the core calculation logic in `stage_calculator.py`, deviating from the original `tat_calculator.py` implementation.

## Key Differences Fixed

### 1. **Fallback Calculation Timing**
**Original Logic (tat_calculator.py):**
- If no precedence_timestamp is available, immediately use fallback calculation
- Fallback is used as an early exit condition

**Previous Modular Logic (stage_calculator.py - WRONG):**
- Fallback was only used at the very end if all other methods failed
- This changed the priority sequence

**Fixed Logic:**
```python
# 2. ORIGINAL LOGIC: If no precedence, use fallback IMMEDIATELY
if not precedence_timestamp:
    fallback_result, fallback_formula = self.expression_evaluator.evaluate_expression(
        stage.fallback_calculation.expression, po_row
    )
    if fallback_result:
        final_timestamp = fallback_result + timedelta(days=stage.lead_time)
        calc_details["method"] = "fallback"
        # Cache result and return (ORIGINAL LOGIC)
        result = (final_timestamp, calc_details)
        self.calculated_adjustments[stage_id] = result
        return result
```

### 2. **Lead Time Application**
**Original Logic:**
- Lead time applied differently based on calculation method
- `precedence_timestamp` stored WITHOUT lead time
- Lead time added when creating final timestamp

**Fixed Logic:**
```python
# Store precedence without lead time (ORIGINAL)
precedence_timestamp = base_timestamp  # WITHOUT lead time here

# Apply lead time when creating final result (ORIGINAL)
if precedence_over_actual:
    final_timestamp = precedence_timestamp + timedelta(days=stage.lead_time)
```

### 3. **Priority Sequence**
**Original Priority:**
1. Calculate precedence (if available)
2. **If no precedence → immediate fallback (early exit)**
3. If precedence available → compare with actual
4. Choose max(precedence, actual) with proper lead time application

**Fixed Logic maintains this exact sequence.**

## Files Modified

### `stage_calculator.py` - PRIMARY FIX
- Restored original fallback timing logic
- Fixed lead time application to match original
- Maintained exact priority sequence from commit f185c4974185bdf01487f0bc911e63116b6d6956
- Added detailed comments marking "ORIGINAL LOGIC" sections

### Other Files (Already Aligned)
- `tat_calculator.py` - **UNCHANGED** (preserves original logic)
- `tat_calculator_main.py` - Good (proper modular wrapper)
- `tat_processor.py` - Good (maintains compatibility)
- `run_tat_calculation.py` - Good (has fallback mechanism)

## Benefits of This Fix

1. **Preserves Original Logic**: Core calculation behavior remains identical to commit f185c4974185bdf01487f0bc911e63116b6d6956
2. **Maintains Modularity**: Code organization benefits are preserved
3. **Backward Compatibility**: Original `tat_calculator.py` still works
4. **Forward Compatibility**: Modular system works with delay analysis and other new features
5. **Enhanced Functionality**: Now includes integrated delay information

## Usage

### Option 1: Use Original Monolithic Version
```python
from tat_calculator import TATCalculator
calculator = TATCalculator("stages_config.json")
```

### Option 2: Use Enhanced Modular Version (Recommended)
```python
from tat_calculator_main import TATCalculator
calculator = TATCalculator("stages_config.json")
result = calculator.calculate_tat(po_row)  # Now includes delay_days and delay_status
```

### Option 3: Use Runner (Most Convenient)
```python
# Automatically chooses best available version
python run_tat_calculation.py
```

## Confidence Level
- **HIGH**: The fix addresses the core logical differences
- **VERIFIED**: Original calculation precedence restored
- **TESTED**: Modular structure maintains compatibility
- **ENHANCED**: Now includes integrated delay information
- **FUTURE-PROOF**: Changes are minimal and preserve both old and new functionality


# Migration Guide: From Monolithic to Enhanced Modular TAT Calculator

## Overview

The TAT Calculator has evolved from a single monolithic file (`tat_calculator.py`) into an enhanced modular structure with integrated delay information. This improves maintainability, testability, code organization, and provides comprehensive delay analytics.

## Key Enhancements

1. **Modular Architecture**: 4 specialized modules + main coordinator
2. **Organized Output Structure**: Clean folder structure for all outputs
3. **Integrated Delay Information**: Delay data included directly in TAT results
4. **Enhanced Excel Exports**: Automatic delay columns in exports
5. **Comprehensive Analytics**: Rich delay summary statistics

## Module Mapping

The original `tat_calculator.py` has been enhanced into:

1. **`models_config.py`** - All Pydantic models and configuration
2. **`expression_evaluator.py`** - Expression evaluation logic
3. **`stage_calculator.py`** - Core stage calculation logic
4. **`tat_processor.py`** - TAT processing with integrated delays
5. **`delay_calculator.py`** - Detailed delay analysis
6. **`tat_calculator_main.py`** - Enhanced main coordinator

## Migration Steps

### For Users

No changes required! Enhanced backward compatibility:

```python
# Old way still works:
from tat_calculator import TATCalculator

# New enhanced way (recommended):
from tat_calculator_main import TATCalculator

# Both work, but new version includes delay information
calculator = TATCalculator()
result = calculator.calculate_tat(po_row)
# result now includes delay_days and delay_status for each stage
```

### For Developers

When making changes:

1. **Configuration changes**: Only edit `stages_config.json`
2. **Expression functions**: Edit `expression_evaluator.py`
3. **Calculation logic**: Edit `stage_calculator.py`
4. **Output formats**: Edit `tat_processor.py`
5. **Delay logic**: Edit `delay_calculator.py`

## Key Benefits

1. **Single Responsibility**: Each module has one clear purpose
2. **Integrated Delays**: Delay information included in TAT results by default
3. **Organized Outputs**: Clean folder structure for all generated files
4. **Enhanced Analytics**: Rich delay summary statistics
5. **Better Excel Reports**: Automatic delay columns in exports
6. **Improved User Experience**: No need to cross-reference separate files

## Important Notes

- All functionality remains the same with enhancements
- No changes to `stages_config.json` format
- Excel input/output formats enhanced with delay columns
- JSON output structure enhanced with delay information
- **New**: Integrated delay information in TAT results
- **New**: Organized folder structure for outputs

## Future Deprecation

The monolithic `tat_calculator.py` will be maintained for 6 months (until December 2025) and then deprecated. Please update any custom scripts to use `tat_calculator_main.py` for the enhanced functionality.

## Enhanced Output Examples

### TAT Results (Enhanced)
```json
{
  "stages": {
    "8": {
      "name": "08. PRD Pending",
      "timestamp": "2025-06-13T00:00:00",
      "delay_days": 5,
      "delay_status": "delayed",
      "delay_reason": "Actual completion 5 days after target"
    }
  },
  "summary": {
    "delay_summary": {
      "delayed_stages": 3,
      "total_delay_days": 15,
      "critical_path_delays": 1
    }
  }
}
```

### Excel Export (Enhanced)
- `08. PRD Pending_Date` - Calculated timestamp
- `08. PRD Pending_Delay_Days` - 5
- `08. PRD Pending_Status` - delayed
