"""
Stage Calculator Module
=======================

Core calculation logic for individual stages with memoization.
This module maintains the exact calculation logic from the original tat_calculator.py
"""

import ast
import logging
from datetime import datetime, timedelta
from typing import Dict, Optional, Tuple, Any
import pandas as pd
from .models_config import StageConfig, StagesConfig
from .expression_evaluator import ExpressionEvaluator

logger = logging.getLogger(__name__)


class StageCalculator:
    """
    Calculates adjusted timestamps for individual stages using the ORIGINAL priority logic:
    
    1. Precedence-based calculation (from dependencies + lead time)
    2. Actual timestamp comparison (if available)
    3. Fallback calculation (when no precedence available)
    
    This maintains the exact logic from the original tat_calculator.py
    """
    
    def __init__(self, config: StagesConfig, expression_evaluator: ExpressionEvaluator):
        self.config = config
        self.expression_evaluator = expression_evaluator
        self.calculated_adjustments: Dict[str, Tuple[Optional[datetime], Dict[str, Any]]] = {}
        # Link the evaluator to our cache
        self.expression_evaluator.set_calculated_adjustments(self.calculated_adjustments)
    
    def calculate_adjusted_timestamp(self, stage_id: str, po_row: pd.Series) -> Tuple[Optional[datetime], Dict[str, Any]]:
        """
        Calculate adjusted timestamp for a specific stage using the ORIGINAL priority logic:
        
        1. Precedence-based calculation (from dependencies + lead time)
        2. Actual timestamp comparison (if available)
        3. Fallback calculation (when no precedence available)
        
        Args:
            stage_id: ID of the stage to calculate
            po_row: PO data row
            
        Returns:
            Tuple of (calculated_timestamp, calculation_details)
        """
        # Check if already calculated (memoization)
        if stage_id in self.calculated_adjustments:
            return self.calculated_adjustments[stage_id]
        
        if stage_id not in self.config.stages:
            logger.error(f"Stage {stage_id} not found in configuration")
            return None, {"method": "error", "reason": f"Stage {stage_id} not found"}
        
        stage = self.config.stages[stage_id]
        
        # Initialize calculation details
        calc_details = {
            "method": None,
            "source": None,
            "target_date": None,
            "lead_time_applied": stage.lead_time,
            "decision_reason": None,
            "dependencies": [],
            "actual_field": None,
            "actual_value": None,
            "precedence_value": None,
            "final_choice": None
        }
        
        # 1. Calculate precedence-based timestamp (ORIGINAL LOGIC)
        precedence_timestamp = None
        if stage.preceding_stage:
            dependencies = []
            preceding_timestamps = []
            
            # Just evaluate the string expression - it always returns a list
            result = self.expression_evaluator._eval_node(
                ast.parse(stage.preceding_stage, mode='eval').body, po_row
            )
            preceding_stage_ids = result if isinstance(result, list) else []
            
            # Process the stage IDs
            for prec_stage_id in preceding_stage_ids:
                prec_stage_id = str(prec_stage_id)
                if prec_stage_id in self.config.stages:
                    prec_timestamp, prec_details = self.calculate_adjusted_timestamp(prec_stage_id, po_row)
                    if prec_timestamp:
                        preceding_timestamps.append(prec_timestamp)
                        dependencies.append({
                            "stage_id": prec_stage_id,
                            "stage_name": self.config.stages[prec_stage_id].name,
                            "timestamp": prec_timestamp.isoformat(),
                            "method": prec_details["method"] if isinstance(prec_details, dict) else "legacy"
                        })
            
            calc_details["dependencies"] = dependencies
            if preceding_timestamps:
                base_timestamp = max(preceding_timestamps)
                precedence_timestamp = base_timestamp  # WITHOUT lead time here (original logic)
                calc_details["precedence_value"] = precedence_timestamp.isoformat()
                calc_details["target_date"] = (base_timestamp + timedelta(days=stage.lead_time)).isoformat()
        
        # 2. ORIGINAL LOGIC: If no precedence, use fallback IMMEDIATELY
        if not precedence_timestamp:
            fallback_result, fallback_formula = self.expression_evaluator.evaluate_expression(
                stage.fallback_calculation.expression, po_row
            )
            print("Fallback", fallback_result)
            if fallback_result:
                final_timestamp = fallback_result + timedelta(days=stage.lead_time)
                calc_details["method"] = "fallback"
                calc_details["source"] = stage.fallback_calculation.expression
                calc_details["target_date"] = fallback_result.isoformat()
                calc_details["decision_reason"] = "No precedence available, using fallback expression"
                calc_details["final_choice"] = "fallback"
                
                # Cache result and return (ORIGINAL LOGIC)
                result = (final_timestamp, calc_details)
                self.calculated_adjustments[stage_id] = result
            else:
                calc_details["method"] = "failed"
                calc_details["decision_reason"] = "No valid calculation method available"
                # Cache and return failure
                result = (None, calc_details)
                self.calculated_adjustments[stage_id] = result
                return result
        
        # 3. Extract and get actual timestamp (ORIGINAL LOGIC)
        actual_timestamp = None
        actual_formula = None
        if stage.actual_timestamp:
            actual_timestamp, actual_formula = self.expression_evaluator.evaluate_expression(
                stage.actual_timestamp, po_row
            )
            calc_details["actual_field"] = stage.actual_timestamp
            if actual_timestamp:
                calc_details["actual_value"] = actual_timestamp.isoformat()
        
        # 4. Determine final timestamp and method (ORIGINAL LOGIC)
        final_timestamp = None
        if precedence_timestamp and actual_timestamp:
            if actual_timestamp >= precedence_timestamp:
                final_timestamp = actual_timestamp
                calc_details["method"] = "actual_over_precedence"
                calc_details["source"] = actual_formula
                calc_details["decision_reason"] = f"Actual date ({actual_timestamp.strftime('%Y-%m-%d')}) is later than precedence date ({precedence_timestamp.strftime('%Y-%m-%d')})"
                calc_details["final_choice"] = "actual"
            else:
                final_timestamp = precedence_timestamp + timedelta(days=stage.lead_time)  # Apply lead time here (ORIGINAL)
                calc_details["method"] = "precedence_over_actual"
                calc_details["source"] = f"Calculated from dependencies"
                calc_details["decision_reason"] = f"Precedence stage's timestamp ({precedence_timestamp.strftime('%Y-%m-%d')}) is later than actual ({actual_timestamp.strftime('%Y-%m-%d')})"
                calc_details["final_choice"] = "precedence"
        
        elif actual_timestamp:
            final_timestamp = actual_timestamp
            calc_details["method"] = "actual_only"
            calc_details["source"] = actual_formula
            calc_details["decision_reason"] = "Using actual timestamp"
            calc_details["final_choice"] = "actual"
        
        elif precedence_timestamp:
            final_timestamp = precedence_timestamp + timedelta(days=stage.lead_time)
            calc_details["method"] = "precedence_only"
            calc_details["source"] = f"Calculated from dependencies + {stage.lead_time} days"
            calc_details["decision_reason"] = "No actual timestamp available, using precedence calculation"
            calc_details["final_choice"] = "precedence"
        
        # Cache result
        result = (final_timestamp, calc_details)
        self.calculated_adjustments[stage_id] = result
        
        return result
    
    def extract_actual_field(self, expression: str) -> Optional[str]:
        """
        Extract the 'actual' field from a max() expression
        
        For expressions like "max(actual_field, fallback)", returns "actual_field"
        """
        try:
            tree = ast.parse(expression, mode='eval')
            if isinstance(tree.body, ast.Call) and isinstance(tree.body.func, ast.Name):
                if tree.body.func.id == 'max' and tree.body.args:
                    first_arg = tree.body.args[0]
                    if isinstance(first_arg, ast.Name):
                        return first_arg.id
        except:
            pass
        return None
    
    def reset_cache(self):
        """Clear the memoization cache for new calculations"""
        self.calculated_adjustments = {}
        self.expression_evaluator.set_calculated_adjustments(self.calculated_adjustments)
