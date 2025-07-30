"""
Enhanced Business Metrics Service
Handles creation, validation, and management of custom business metrics
with formula helpers and DuckDB integration for LLM queries
"""

import logging
import re
import json
import duckdb
from typing import Dict, List, Tuple, Optional, Any
from django.db import transaction
from django.conf import settings
from django.core.cache import cache
from datasets.models import SemanticMetric, SemanticTable, SemanticColumn
from django.contrib.auth import get_user_model
import os

logger = logging.getLogger(__name__)
User = get_user_model()

class BusinessMetricsService:
    """Enhanced service for managing business metrics with validation and LLM integration"""
    
    def __init__(self):
        self.duckdb_path = os.path.join(settings.BASE_DIR, 'data', 'integrated.duckdb')
        self._ensure_duckdb_tables()
        
        # SQL functions and operators for validation
        self.allowed_functions = {
            'SUM', 'AVG', 'COUNT', 'MIN', 'MAX', 'DISTINCT', 'ROUND', 'ABS',
            'CAST', 'COALESCE', 'NULLIF', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END',
            'UPPER', 'LOWER', 'TRIM', 'SUBSTRING', 'LENGTH', 'CONCAT',
            'EXTRACT', 'DATE_PART', 'DATE_TRUNC', 'NOW', 'CURRENT_DATE',
            'LAG', 'LEAD', 'ROW_NUMBER', 'RANK', 'DENSE_RANK'
        }
        
        self.allowed_operators = {'+', '-', '*', '/', '=', '<', '>', '<=', '>=', '!=', '<>', 'AND', 'OR', 'NOT', 'IN', 'LIKE', 'BETWEEN'}
        
        self.dangerous_keywords = {'DROP', 'DELETE', 'INSERT', 'UPDATE', 'ALTER', 'CREATE', 'TRUNCATE', 'EXEC', 'EXECUTE'}
        
        # Generic metric templates (no hardcoded column names)
        self.metric_templates = {
            'count': {
                'name': 'Record Count',
                'formula': 'COUNT(*)',
                'description': 'Total number of records',
                'category': 'simple'
            },
            'sum_template': {
                'name': 'Sum Total',
                'formula': 'SUM({column})',
                'description': 'Sum of all values in a numeric column',
                'category': 'simple'
            },
            'average_template': {
                'name': 'Average',
                'formula': 'AVG({column})',
                'description': 'Average value of a numeric column',
                'category': 'simple'
            },
            'percentage_template': {
                'name': 'Percentage',
                'formula': '({numerator} / {denominator}) * 100',
                'description': 'Percentage calculation between two values',
                'category': 'ratio'
            },
            'distinct_count_template': {
                'name': 'Distinct Count',
                'formula': 'COUNT(DISTINCT {column})',
                'description': 'Count of unique values in a column',
                'category': 'simple'
            },
            'min_max_template': {
                'name': 'Min/Max',
                'formula': 'MIN({column}), MAX({column})',
                'description': 'Minimum and maximum values',
                'category': 'simple'
            }
        }
    
    def _ensure_duckdb_tables(self):
        """Ensure business metrics tables exist in DuckDB"""
        try:
            os.makedirs(os.path.dirname(self.duckdb_path), exist_ok=True)
            
            with duckdb.connect(self.duckdb_path) as conn:
                # Create business metrics table
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS business_metrics (
                        id VARCHAR,
                        name VARCHAR,
                        display_name VARCHAR,
                        description TEXT,
                        metric_type VARCHAR,
                        calculation TEXT,
                        unit VARCHAR,
                        base_table VARCHAR,
                        created_by VARCHAR,
                        created_at TIMESTAMP,
                        is_active BOOLEAN,
                        validation_rules TEXT,
                        dependent_columns TEXT
                    )
                """)
                
                # Create metrics history table for versioning
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS business_metrics_history (
                        id VARCHAR,
                        metric_id VARCHAR,
                        change_type VARCHAR,
                        old_calculation TEXT,
                        new_calculation TEXT,
                        changed_by VARCHAR,
                        changed_at TIMESTAMP,
                        reason TEXT
                    )
                """)
                
                logger.info("DuckDB business metrics tables ensured")
                
        except Exception as e:
            logger.error(f"Error ensuring DuckDB tables: {e}")
    
    def validate_formula(self, formula: str, table_name: Optional[str] = None) -> Tuple[bool, str, List[str]]:
        """
        Comprehensive formula validation with helpful suggestions
        
        Returns:
            (is_valid, error_message, suggestions)
        """
        try:
            if not formula or not formula.strip():
                return False, "Formula cannot be empty", ["Try: COUNT(*)", "Try: SUM(column_name)", "Try: AVG(column_name)"]
            
            formula_upper = formula.upper()
            suggestions = []
            
            # Check for dangerous keywords
            for keyword in self.dangerous_keywords:
                if keyword in formula_upper:
                    return False, f"Dangerous keyword '{keyword}' not allowed in formulas", [
                        "Use only SELECT-based calculations",
                        "Avoid data modification commands",
                        "Focus on aggregation functions like SUM, COUNT, AVG"
                    ]
            
            # Check for basic syntax issues
            if formula.count('(') != formula.count(')'):
                return False, "Mismatched parentheses in formula", [
                    "Check all opening '(' have closing ')'",
                    "Example: SUM(column_name)",
                    "Example: (column1 + column2) / 2"
                ]
            
            # Check for valid functions and suggest improvements
            used_functions = re.findall(r'\b([A-Z_]+)\s*\(', formula_upper)
            invalid_functions = [f for f in used_functions if f not in self.allowed_functions]
            
            if invalid_functions:
                return False, f"Invalid functions: {', '.join(invalid_functions)}", [
                    f"Available functions: {', '.join(sorted(self.allowed_functions))}",
                    "Use SUM() for totals, COUNT() for counts, AVG() for averages",
                    "Try CASE WHEN condition THEN value END for conditional logic"
                ]
            
            # Validate against table columns if table provided
            if table_name:
                column_suggestions = self._get_column_suggestions(formula, table_name)
                suggestions.extend(column_suggestions)
            
            # Test formula syntax with DuckDB
            validation_result = self._test_formula_syntax(formula, table_name)
            if not validation_result[0]:
                return False, validation_result[1], [
                    "Check SQL syntax",
                    "Ensure column names exist",
                    "Verify function usage"
                ]
            
            return True, "Formula is valid", suggestions
            
        except Exception as e:
            return False, f"Validation error: {str(e)}", [
                "Check formula syntax",
                "Ensure proper SQL format",
                "Contact support if issue persists"
            ]
    
    def _get_column_suggestions(self, formula: str, table_name: str) -> List[str]:
        """Get column-based suggestions for formula improvement"""
        suggestions = []
        
        try:
            # Get available columns for the table
            semantic_table = SemanticTable.objects.filter(name=table_name).first()
            if semantic_table:
                columns = SemanticColumn.objects.filter(semantic_table=semantic_table)
                
                numeric_columns = [col.name for col in columns if col.data_type in ['integer', 'float']]
                text_columns = [col.name for col in columns if col.data_type == 'string']
                date_columns = [col.name for col in columns if col.data_type in ['date', 'datetime']]
                
                if numeric_columns:
                    suggestions.append(f"Numeric columns available: {', '.join(numeric_columns[:5])}")
                if date_columns:
                    suggestions.append(f"Date columns available: {', '.join(date_columns[:3])}")
                if text_columns:
                    suggestions.append(f"Text columns available: {', '.join(text_columns[:3])}")
                    
        except Exception as e:
            logger.warning(f"Error getting column suggestions: {e}")
            
        return suggestions
    
    def _test_formula_syntax(self, formula: str, table_name: Optional[str] = None) -> Tuple[bool, str]:
        """Test formula syntax using DuckDB"""
        try:
            with duckdb.connect(self.duckdb_path) as conn:
                if table_name:
                    # Test with actual table if it exists
                    try:
                        test_query = f"SELECT {formula} as test_metric FROM {table_name} LIMIT 1"
                        conn.execute(test_query)
                        return True, "Syntax valid"
                    except Exception:
                        # Table might not exist in DuckDB, try generic validation
                        pass
                
                # Generic syntax validation
                try:
                    test_query = f"SELECT {formula} as test_metric"
                    conn.execute(test_query)
                    return True, "Syntax valid"
                except Exception as e:
                    return False, f"Syntax error: {str(e)}"
                    
        except Exception as e:
            return False, f"Validation error: {str(e)}"
    
    def get_formula_suggestions(self, partial_formula: str, table_name: Optional[str] = None) -> List[Dict[str, str]]:
        """Get autocomplete suggestions for formula building"""
        suggestions = []
        
        partial_upper = partial_formula.upper()
        
        # Function suggestions
        for func in self.allowed_functions:
            if func.startswith(partial_upper) or partial_upper in func:
                suggestions.append({
                    'type': 'function',
                    'text': func + '(',
                    'description': self._get_function_description(func)
                })
        
        # Column suggestions if table provided
        if table_name:
            try:
                semantic_table = SemanticTable.objects.filter(name=table_name).first()
                if semantic_table:
                    columns = SemanticColumn.objects.filter(semantic_table=semantic_table)
                    for col in columns:
                        # Match by column name or display name
                        if (partial_formula.lower() in col.name.lower() or 
                            partial_formula.lower() in col.display_name.lower()):
                            
                            # Add semantic type info for better context
                            semantic_info = f" - {col.semantic_type}"
                            if col.is_measure:
                                semantic_info += " (measure)"
                            
                            suggestions.append({
                                'type': 'column',
                                'text': col.name,
                                'description': f"{col.display_name} ({col.data_type}){semantic_info}"
                            })
                    
                    # Add common aggregation suggestions for measures
                    measure_columns = [col for col in columns if col.is_measure or col.semantic_type == 'measure']
                    if measure_columns and any(func in partial_upper for func in ['SUM', 'AVG', 'COUNT', 'MIN', 'MAX']):
                        for col in measure_columns[:3]:  # Limit to top 3 measures
                            for func in ['SUM', 'AVG', 'MIN', 'MAX']:
                                if func in partial_upper:
                                    suggestions.append({
                                        'type': 'template',
                                        'text': f'{func}({col.name})',
                                        'description': f'{func} of {col.display_name}'
                                    })
                                    
            except Exception as e:
                logger.warning(f"Error getting column suggestions: {e}")
        
        # Template suggestions
        for template_key, template in self.metric_templates.items():
            if partial_upper in template['name'].upper():
                suggestions.append({
                    'type': 'template',
                    'text': template['formula'],
                    'description': template['description']
                })
        
        return suggestions[:10]  # Limit to 10 suggestions
    
    def _get_function_description(self, function_name: str) -> str:
        """Get description for SQL functions"""
        descriptions = {
            'SUM': 'Calculate total sum of values',
            'AVG': 'Calculate average of values',
            'COUNT': 'Count number of records',
            'MIN': 'Find minimum value',
            'MAX': 'Find maximum value',
            'DISTINCT': 'Get unique values only',
            'ROUND': 'Round to specified decimal places',
            'CASE': 'Conditional logic (CASE WHEN ... THEN ... END)',
            'COALESCE': 'Return first non-null value',
            'EXTRACT': 'Extract part from date/time',
            'CONCAT': 'Concatenate text values'
        }
        return descriptions.get(function_name, f'{function_name} function')
    
    def create_custom_metric(self, name: str, display_name: str, description: str,
                           metric_type: str, calculation: str, unit: str = "",
                           base_table_id: Optional[str] = None, user_id: Optional[int] = None) -> Tuple[bool, str, Optional[str]]:
        """
        Create a new custom business metric with validation
        
        Returns:
            (success, message, metric_id)
        """
        try:
            # Validate inputs
            if not name or not display_name or not calculation:
                return False, "Name, display name, and calculation are required", None
            
            if metric_type not in ['simple', 'calculated', 'ratio', 'growth']:
                return False, "Invalid metric type", None
            
            # Get base table name for validation
            table_name = None
            base_table = None
            if base_table_id:
                try:
                    base_table = SemanticTable.objects.get(id=base_table_id)
                    table_name = base_table.name
                except SemanticTable.DoesNotExist:
                    return False, "Base table not found", None
            
            # Validate formula
            is_valid, validation_msg, suggestions = self.validate_formula(calculation, table_name)
            if not is_valid:
                return False, f"Formula validation failed: {validation_msg}", None
            
            # Create metric in Django database
            with transaction.atomic():
                metric = SemanticMetric.objects.create(
                    name=name,
                    display_name=display_name,
                    description=description,
                    metric_type=metric_type,
                    calculation=calculation,
                    unit=unit,
                    base_table=base_table,
                    created_by_id=user_id or 1,
                    is_active=True,
                    validation_rules=suggestions if suggestions else [],
                    business_owner="",
                    format_string="",
                    tags=[]
                )
                
                # Store in DuckDB for LLM integration
                self._store_metric_in_duckdb(metric)
                
                # Clear caches
                cache.delete('business_metrics_cache')
                cache.delete(f'table_metrics_{table_name}')
                
                logger.info(f"Created custom business metric: {name} (ID: {metric.id})")
                return True, "Metric created successfully", str(metric.id)
                
        except Exception as e:
            logger.error(f"Error creating custom metric: {e}")
            return False, f"Error creating metric: {str(e)}", None
    
    def _store_metric_in_duckdb(self, metric: SemanticMetric):
        """Store metric in DuckDB for LLM queries"""
        try:
            with duckdb.connect(self.duckdb_path) as conn:
                # Convert metric to DuckDB format
                metric_data = {
                    'id': str(metric.id),
                    'name': metric.name,
                    'display_name': metric.display_name,
                    'description': metric.description or '',
                    'metric_type': metric.metric_type,
                    'calculation': metric.calculation,
                    'unit': metric.unit or '',
                    'base_table': metric.base_table.name if metric.base_table else '',
                    'created_by': metric.created_by.username if metric.created_by else '',
                    'created_at': metric.created_at,
                    'is_active': metric.is_active,
                    'validation_rules': metric.validation_rules or '[]',
                    'dependent_columns': ''  # Will be populated later if needed
                }
                
                # Insert or update in DuckDB
                conn.execute("""
                    INSERT OR REPLACE INTO business_metrics VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, list(metric_data.values()))
                
                logger.debug(f"Stored metric {metric.name} in DuckDB")
                
        except Exception as e:
            logger.error(f"Error storing metric in DuckDB: {e}")
    
    def get_metrics_for_llm(self) -> List[Dict[str, Any]]:
        """Get all business metrics formatted for LLM consumption"""
        try:
            with duckdb.connect(self.duckdb_path) as conn:
                result = conn.execute("""
                    SELECT id, name, display_name, description, metric_type, 
                           calculation, unit, base_table
                    FROM business_metrics 
                    WHERE is_active = true
                    ORDER BY display_name
                """).fetchall()
                
                metrics = []
                for row in result:
                    metrics.append({
                        'id': row[0],
                        'name': row[1],
                        'display_name': row[2],
                        'description': row[3],
                        'type': row[4],
                        'formula': row[5],
                        'unit': row[6],
                        'table': row[7]
                    })
                
                return metrics
                
        except Exception as e:
            logger.error(f"Error getting metrics for LLM: {e}")
            return []
    
    def update_metric(self, metric_id: str, **kwargs) -> Tuple[bool, str]:
        """Update an existing metric with validation"""
        try:
            metric = SemanticMetric.objects.get(id=metric_id)
            
            # Store old values for history
            old_calculation = metric.calculation
            
            # Update fields
            for field, value in kwargs.items():
                if hasattr(metric, field):
                    setattr(metric, field, value)
            
            # Validate new calculation if provided
            if 'calculation' in kwargs:
                table_name = metric.base_table.name if metric.base_table else None
                is_valid, validation_msg, _ = self.validate_formula(kwargs['calculation'], table_name)
                if not is_valid:
                    return False, f"Formula validation failed: {validation_msg}"
            
            metric.save()
            
            # Update DuckDB
            self._store_metric_in_duckdb(metric)
            
            # Store change history
            if 'calculation' in kwargs and old_calculation != kwargs['calculation']:
                self._store_metric_history(metric_id, 'update', old_calculation, kwargs['calculation'])
            
            # Clear caches
            cache.delete('business_metrics_cache')
            
            return True, "Metric updated successfully"
            
        except SemanticMetric.DoesNotExist:
            return False, "Metric not found"
        except Exception as e:
            logger.error(f"Error updating metric {metric_id}: {e}")
            return False, f"Error updating metric: {str(e)}"
    
    def _store_metric_history(self, metric_id: str, change_type: str, old_calc: str, new_calc: str, reason: str = ""):
        """Store metric change history"""
        try:
            with duckdb.connect(self.duckdb_path) as conn:
                conn.execute("""
                    INSERT INTO business_metrics_history 
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, [
                    str(uuid.uuid4()), metric_id, change_type, old_calc, new_calc,
                    'system', datetime.now(), reason
                ])
        except Exception as e:
            logger.warning(f"Error storing metric history: {e}")
    
    def delete_metric(self, metric_id: str) -> Tuple[bool, str]:
        """Delete a metric and its DuckDB entry"""
        try:
            metric = SemanticMetric.objects.get(id=metric_id)
            metric_name = metric.name
            
            # Delete from Django
            metric.delete()
            
            # Delete from DuckDB
            with duckdb.connect(self.duckdb_path) as conn:
                conn.execute("DELETE FROM business_metrics WHERE id = ?", [metric_id])
            
            # Clear caches
            cache.delete('business_metrics_cache')
            
            logger.info(f"Deleted metric: {metric_name}")
            return True, "Metric deleted successfully"
            
        except SemanticMetric.DoesNotExist:
            return False, "Metric not found"
        except Exception as e:
            logger.error(f"Error deleting metric {metric_id}: {e}")
            return False, f"Error deleting metric: {str(e)}"
    
    def test_metric_calculation(self, calculation: str, table_name: str, limit: int = 5) -> Tuple[bool, str, Any]:
        """Test a metric calculation against real data"""
        try:
            with duckdb.connect(self.duckdb_path) as conn:
                test_query = f"""
                    SELECT {calculation} as metric_value 
                    FROM {table_name} 
                    LIMIT {limit}
                """
                
                result = conn.execute(test_query).fetchall()
                
                if result:
                    return True, "Test successful", result
                else:
                    return False, "No data returned", None
                    
        except Exception as e:
            return False, f"Test failed: {str(e)}", None

# Add required imports
import uuid
from datetime import datetime 