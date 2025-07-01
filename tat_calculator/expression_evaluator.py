"""
Expression Evaluation Engine
============================

Dynamic expression evaluation with custom functions for TAT calculations.
"""

import ast
import logging
from datetime import datetime, timedelta
from typing import Any, Optional, Tuple
import pandas as pd

logger = logging.getLogger(__name__)


class ExpressionEvaluator:
    """
    Evaluates dynamic expressions with custom functions.
    
    Supported functions:
    - max(date1, date2, ...): Returns latest datetime
    - add_days(date, days): Adds days to a date
    - cond(condition, true_val, false_val): Conditional evaluation
    """
    
    def __init__(self):
        self.calculated_adjustments = {}
    
    def set_calculated_adjustments(self, adjustments):
        """Set the calculated adjustments cache for stage references"""
        self.calculated_adjustments = adjustments
    
    def get_date_value(self, field_name: str, po_row: pd.Series) -> Optional[datetime]:
        """
        Extract datetime value from PO data with robust handling
        
        Args:
            field_name: Name of the field to extract
            po_row: Pandas Series containing PO data
            
        Returns:
            datetime object or None if not available/valid
        """
        if field_name not in po_row.index:
            logger.warning(f"Field '{field_name}' not found in PO data")
            return None
            
        value = po_row[field_name]
        
        # Handle various data types
        if pd.isna(value) or value == "" or value == "NA":
            return None
            
        if isinstance(value, datetime):
            return value
            
        if isinstance(value, str):
            try:
                # Try common datetime formats
                for fmt in ["%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%m/%d/%Y", "%d/%m/%Y"]:
                    try:
                        return datetime.strptime(value, fmt)
                    except ValueError:
                        continue
                        
                # Try pandas datetime parsing as fallback
                return pd.to_datetime(value)
            except:
                logger.warning(f"Could not parse date from field '{field_name}': {value}")
                return None
                
        return None
    
    def evaluate_expression(self, expression: str, po_row: pd.Series) -> Tuple[Optional[datetime], str]:
        """
        Evaluate dynamic expressions with custom functions
        
        Args:
            expression: Expression string to evaluate
            po_row: PO data for field resolution
            
        Returns:
            Tuple of (result_datetime, formula_description)
        """
        try:
            # Parse expression into AST
            tree = ast.parse(expression, mode='eval')
            result = self._eval_node(tree.body, po_row)
            
            if isinstance(result, datetime):
                return result, f"Calculation: {expression} = {result.strftime('%Y-%m-%d %H:%M:%S')}"
            else:
                return None, f"Calculation failed: {expression}"
                
        except Exception as e:
            logger.error(f"Error evaluating expression '{expression}': {e}")
            return None, f"Calculation error: {expression} ({str(e)})"
    
    def _eval_node(self, node: ast.AST, po_row: pd.Series) -> Any:
        """Recursively evaluate AST nodes"""
        if isinstance(node, ast.Name):
            var_name = node.id
            # Check if it's a stage reference
            if var_name.startswith('stage_'):
                print("Variable name is", var_name)
                stage_id = var_name.replace('stage_', '')
                if stage_id in self.calculated_adjustments:
                    timestamp, _ = self.calculated_adjustments[stage_id]
                    return timestamp  # Already a datetime or None
            
            return po_row.get(node.id)
        
        elif isinstance(node, ast.Constant):
            return node.value
        
        elif isinstance(node, ast.List):
            # Handle list literals like ['5'], ['2']
            return [self._eval_node(elt, po_row) for elt in node.elts]
        
        elif isinstance(node, ast.BinOp):
            # Handle binary operations like +, -, *, /
            left = self._eval_node(node.left, po_row)
            right = self._eval_node(node.right, po_row)
            
            if left is None or right is None:
                return None
            
            # Handle datetime arithmetic
            if isinstance(node.op, ast.Add):
                # For datetime + number (days)
                if isinstance(left, datetime) and isinstance(right, (int, float)):
                    return left + timedelta(days=right)
                elif isinstance(right, datetime) and isinstance(left, (int, float)):
                    return right + timedelta(days=left)
                else:
                    return left + right
                    
            elif isinstance(node.op, ast.Sub):
                # For datetime - number (days)
                if isinstance(left, datetime) and isinstance(right, (int, float)):
                    return left - timedelta(days=right)
                # For datetime - datetime (returns days)
                elif isinstance(left, datetime) and isinstance(right, datetime):
                    return (left - right).days
                else:
                    return left - right
                    
            elif isinstance(node.op, ast.Mult):
                return left * right
                
            elif isinstance(node.op, ast.Div):
                return left / right if right != 0 else None
                
            else:
                logger.warning(f"Unsupported binary operation: {type(node.op)}")
                return None
        
        elif isinstance(node, ast.Compare):
            # Handle comparisons like pi_applicable==1
            left = self._eval_node(node.left, po_row)
            op = node.ops[0]
            right = self._eval_node(node.comparators[0], po_row)
            
            if isinstance(op, ast.Eq):
                return left == right
            elif isinstance(op, ast.NotEq):
                return left != right
            elif isinstance(op, ast.Lt):
                return left < right
            elif isinstance(op, ast.LtE):
                return left <= right
            elif isinstance(op, ast.Gt):
                return left > right
            elif isinstance(op, ast.GtE):
                return left >= right
            else:
                raise ValueError(f"Unsupported comparison operator: {type(op).__name__}")
        
        elif isinstance(node, ast.Call):
            if isinstance(node.func, ast.Name):
                func_name = node.func.id
                
                # Handle iff with lazy evaluation
                if func_name == 'iff' or func_name == 'cond':
                    if len(node.args) == 3:
                        condition = self._eval_node(node.args[0], po_row)
                        if condition:
                            return self._eval_node(node.args[1], po_row)
                        else:
                            return self._eval_node(node.args[2], po_row)
                    raise ValueError("iff requires exactly 3 arguments")
                
                # For other functions, evaluate arguments first
                args = [self._eval_node(arg, po_row) for arg in node.args]
                
                if func_name == 'max':
                    valid_dates = [arg for arg in args if isinstance(arg, datetime)]
                    return max(valid_dates) if valid_dates else None
                
                elif func_name == 'add_days':
                    if len(args) >= 2 and isinstance(args[0], datetime) and isinstance(args[1], (int, float)):
                        return args[0] + timedelta(days=int(args[1]))
                    return None
                
                else:
                    raise ValueError(f"Unknown function: {func_name}")
        
        else:
            raise ValueError(f"Unsupported AST node type: {type(node).__name__}")
