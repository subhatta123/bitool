"""
DataFrame Validator Utility for ConvaBI Application
Provides comprehensive DataFrame validation and safety checks to prevent boolean context ambiguity errors
"""

import pandas as pd
import numpy as np
from typing import Any, Dict, List, Tuple, Optional, Union
import logging
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)


class DataFrameSafetyLevel(Enum):
    """Enum for different levels of DataFrame safety checking"""
    MINIMAL = "minimal"
    STANDARD = "standard"
    STRICT = "strict"
    PARANOID = "paranoid"


@dataclass
class DataFrameValidationResult:
    """Result of DataFrame validation"""
    is_safe: bool
    issues_found: List[str]
    warnings: List[str]
    safe_representation: Optional[Any] = None
    metadata: Optional[Dict[str, Any]] = None


class DataFrameValidator:
    """
    Comprehensive DataFrame validator that prevents boolean context ambiguity errors
    """
    
    def __init__(self, safety_level: DataFrameSafetyLevel = DataFrameSafetyLevel.STANDARD):
        self.safety_level = safety_level
        self.validation_stats = {
            'total_validations': 0,
            'dataframes_found': 0,
            'ambiguity_issues_resolved': 0,
            'warnings_generated': 0
        }
    
    def validate_dataframe_safe(self, obj: Any, path: str = "root") -> DataFrameValidationResult:
        """
        Recursively validate that an object doesn't contain DataFrame boolean context issues
        
        Args:
            obj: Object to validate
            path: Path to current object (for debugging)
            
        Returns:
            DataFrameValidationResult with validation details
        """
        self.validation_stats['total_validations'] += 1
        
        issues = []
        warnings = []
        
        try:
            # Check if object is a DataFrame
            if isinstance(obj, pd.DataFrame):
                self.validation_stats['dataframes_found'] += 1
                return self._validate_single_dataframe(obj, path)
            
            # Check if object is a Series
            elif isinstance(obj, pd.Series):
                return self._validate_series(obj, path)
            
            # Recursively check containers
            elif isinstance(obj, dict):
                return self._validate_dict(obj, path)
            
            elif isinstance(obj, (list, tuple)):
                return self._validate_sequence(obj, path)
            
            else:
                # Object is safe (not a pandas object)
                return DataFrameValidationResult(
                    is_safe=True,
                    issues_found=[],
                    warnings=[],
                    safe_representation=obj
                )
                
        except Exception as e:
            logger.error(f"Error validating object at {path}: {e}")
            issues.append(f"Validation error at {path}: {str(e)}")
            return DataFrameValidationResult(
                is_safe=False,
                issues_found=issues,
                warnings=warnings,
                safe_representation=str(obj) if obj is not None else None
            )
    
    def _validate_single_dataframe(self, df: pd.DataFrame, path: str) -> DataFrameValidationResult:
        """Validate a single DataFrame object"""
        issues = []
        warnings = []
        
        try:
            # Test for boolean context ambiguity
            if self._is_dataframe_in_boolean_context(df):
                issues.append(f"DataFrame at {path} may cause boolean context ambiguity")
                self.validation_stats['ambiguity_issues_resolved'] += 1
            
            # Get safe representation
            safe_repr = self._convert_dataframe_to_safe(df)
            
            # Additional validations based on safety level
            if self.safety_level in [DataFrameSafetyLevel.STRICT, DataFrameSafetyLevel.PARANOID]:
                # Check for problematic data types
                object_cols = df.select_dtypes(include=['object']).columns
                if len(object_cols) > 0:
                    warnings.append(f"DataFrame at {path} contains object columns: {list(object_cols)}")
                    self.validation_stats['warnings_generated'] += 1
                
                # Check for very large DataFrames that might cause memory issues
                if df.memory_usage(deep=True).sum() > 100 * 1024 * 1024:  # 100MB
                    warnings.append(f"DataFrame at {path} is very large ({df.memory_usage(deep=True).sum() / 1024 / 1024:.1f}MB)")
                    self.validation_stats['warnings_generated'] += 1
            
            # Check if DataFrame is safe for serialization
            is_safe = len(issues) == 0
            
            return DataFrameValidationResult(
                is_safe=is_safe,
                issues_found=issues,
                warnings=warnings,
                safe_representation=safe_repr,
                metadata={
                    'shape': df.shape,
                    'columns': list(df.columns),
                    'dtypes': {col: str(dtype) for col, dtype in df.dtypes.items()},
                    'memory_usage_mb': df.memory_usage(deep=True).sum() / 1024 / 1024
                }
            )
            
        except Exception as e:
            logger.error(f"Error validating DataFrame at {path}: {e}")
            return DataFrameValidationResult(
                is_safe=False,
                issues_found=[f"DataFrame validation error: {str(e)}"],
                warnings=warnings,
                safe_representation=f"DataFrame({df.shape[0]}x{df.shape[1]}) - validation error"
            )
    
    def _validate_series(self, series: pd.Series, path: str) -> DataFrameValidationResult:
        """Validate a pandas Series object"""
        try:
            safe_repr = series.tolist() if len(series) <= 1000 else f"Series({len(series)} elements)"
            
            return DataFrameValidationResult(
                is_safe=True,
                issues_found=[],
                warnings=[] if len(series) <= 1000 else [f"Large Series at {path} converted to summary"],
                safe_representation=safe_repr,
                metadata={'length': len(series), 'dtype': str(series.dtype)}
            )
        except Exception as e:
            return DataFrameValidationResult(
                is_safe=False,
                issues_found=[f"Series validation error: {str(e)}"],
                warnings=[],
                safe_representation=f"Series({len(series)} elements) - error"
            )
    
    def _validate_dict(self, obj: dict, path: str) -> DataFrameValidationResult:
        """Validate a dictionary that might contain DataFrames"""
        issues = []
        warnings = []
        safe_dict = {}
        
        for key, value in obj.items():
            key_path = f"{path}.{key}"
            
            try:
                result = self.validate_dataframe_safe(value, key_path)
                
                issues.extend(result.issues_found)
                warnings.extend(result.warnings)
                safe_dict[key] = result.safe_representation
                
            except Exception as e:
                logger.error(f"Error validating dict key {key_path}: {e}")
                issues.append(f"Dict validation error at {key_path}: {str(e)}")
                safe_dict[key] = str(value) if value is not None else None
        
        return DataFrameValidationResult(
            is_safe=len(issues) == 0,
            issues_found=issues,
            warnings=warnings,
            safe_representation=safe_dict
        )
    
    def _validate_sequence(self, obj: Union[list, tuple], path: str) -> DataFrameValidationResult:
        """Validate a list or tuple that might contain DataFrames"""
        issues = []
        warnings = []
        safe_items = []
        
        for i, item in enumerate(obj):
            item_path = f"{path}[{i}]"
            
            try:
                result = self.validate_dataframe_safe(item, item_path)
                
                issues.extend(result.issues_found)
                warnings.extend(result.warnings)
                safe_items.append(result.safe_representation)
                
            except Exception as e:
                logger.error(f"Error validating sequence item {item_path}: {e}")
                issues.append(f"Sequence validation error at {item_path}: {str(e)}")
                safe_items.append(str(item) if item is not None else None)
        
        safe_representation = safe_items if isinstance(obj, list) else tuple(safe_items)
        
        return DataFrameValidationResult(
            is_safe=len(issues) == 0,
            issues_found=issues,
            warnings=warnings,
            safe_representation=safe_representation
        )
    
    def _is_dataframe_in_boolean_context(self, df: pd.DataFrame) -> bool:
        """
        Detect if a DataFrame might be used in a boolean context that could cause ambiguity
        
        Args:
            df: DataFrame to check
            
        Returns:
            True if DataFrame might cause boolean context issues
        """
        try:
            # Test potential boolean context scenarios
            
            # 1. Test empty check (this should work fine)
            _ = df.empty
            
            # 2. Test shape-based checks (safer alternative)
            _ = df.shape[0] > 0
            
            # 3. Check if DataFrame has problematic structure that might trigger ambiguity
            if df.shape[0] == 1 and df.shape[1] == 1:
                # Single cell DataFrames might be more prone to ambiguity
                return True
            
            # If we get here without errors, DataFrame is likely safe
            return False
            
        except ValueError as e:
            if 'ambiguous' in str(e).lower():
                return True
            return False
        except Exception:
            # Other errors might indicate problematic DataFrame
            return True
    
    def _convert_dataframe_to_safe(self, df: pd.DataFrame) -> Any:
        """
        Convert DataFrame to a safe representation for serialization
        
        Args:
            df: DataFrame to convert
            
        Returns:
            Safe representation of the DataFrame
        """
        try:
            # Use shape check instead of .empty to avoid ambiguity
            if df.shape[0] > 0:
                # For large DataFrames, limit the conversion
                if df.shape[0] > 10000:
                    logger.warning(f"Large DataFrame ({df.shape}) being converted, taking sample")
                    sample_df = df.head(1000)
                    return {
                        'sample_data': sample_df.to_dict('records'),
                        'total_rows': df.shape[0],
                        'total_columns': df.shape[1],
                        'is_sample': True
                    }
                else:
                    return df.to_dict('records')
            else:
                return []
                
        except ValueError as e:
            if 'ambiguous' in str(e).lower():
                # Handle ambiguous boolean context by using shape
                logger.warning("DataFrame boolean ambiguity handled during conversion")
                try:
                    return df.to_dict('records') if df.shape[0] > 0 else []
                except Exception as fallback_error:
                    logger.error(f"DataFrame fallback conversion failed: {fallback_error}")
                    return f"DataFrame({df.shape[0]}x{df.shape[1]}) - conversion error"
            else:
                raise
        except Exception as e:
            logger.error(f"DataFrame conversion error: {e}")
            return f"DataFrame({df.shape[0]}x{df.shape[1]}) - conversion error: {str(e)}"
    
    def convert_dataframes_to_safe(self, obj: Any) -> Any:
        """
        Convert any DataFrames in an object to safe representations
        
        Args:
            obj: Object that might contain DataFrames
            
        Returns:
            Object with DataFrames converted to safe representations
        """
        result = self.validate_dataframe_safe(obj)
        return result.safe_representation
    
    def get_validation_stats(self) -> Dict[str, int]:
        """Get validation statistics"""
        return self.validation_stats.copy()
    
    def reset_stats(self):
        """Reset validation statistics"""
        self.validation_stats = {
            'total_validations': 0,
            'dataframes_found': 0,
            'ambiguity_issues_resolved': 0,
            'warnings_generated': 0
        }


# Convenience functions for common use cases

def validate_dataframe_safe(obj: Any, safety_level: DataFrameSafetyLevel = DataFrameSafetyLevel.STANDARD) -> DataFrameValidationResult:
    """
    Validate that an object doesn't contain DataFrame boolean context issues
    
    Args:
        obj: Object to validate
        safety_level: Level of safety checking to perform
        
    Returns:
        DataFrameValidationResult with validation details
    """
    validator = DataFrameValidator(safety_level)
    return validator.validate_dataframe_safe(obj)


def convert_dataframes_to_safe(obj: Any, safety_level: DataFrameSafetyLevel = DataFrameSafetyLevel.STANDARD) -> Any:
    """
    Convert any DataFrames in an object to safe representations
    
    Args:
        obj: Object that might contain DataFrames
        safety_level: Level of safety checking to perform
        
    Returns:
        Object with DataFrames converted to safe representations
    """
    validator = DataFrameValidator(safety_level)
    return validator.convert_dataframes_to_safe(obj)


def is_dataframe_in_boolean_context(df: pd.DataFrame) -> bool:
    """
    Detect if a DataFrame might be used in a boolean context that could cause ambiguity
    
    Args:
        df: DataFrame to check
        
    Returns:
        True if DataFrame might cause boolean context issues
    """
    validator = DataFrameValidator()
    return validator._is_dataframe_in_boolean_context(df)


# Performance-optimized validators for large datasets

class LightweightDataFrameValidator:
    """
    Lightweight DataFrame validator for performance-critical scenarios
    """
    
    @staticmethod
    def quick_dataframe_check(obj: Any) -> bool:
        """
        Quick check if object contains DataFrames (for performance-critical paths)
        
        Args:
            obj: Object to check
            
        Returns:
            True if object is safe (no DataFrames found)
        """
        try:
            if isinstance(obj, pd.DataFrame):
                return False  # DataFrame found
            elif isinstance(obj, dict):
                return all(not isinstance(v, pd.DataFrame) for v in obj.values())
            elif isinstance(obj, (list, tuple)):
                return all(not isinstance(item, pd.DataFrame) for item in obj)
            else:
                return True  # Safe
        except Exception:
            return False  # Error means potentially unsafe
    
    @staticmethod
    def safe_dataframe_empty_check(df: pd.DataFrame) -> bool:
        """
        Safe way to check if DataFrame is empty without boolean context issues
        
        Args:
            df: DataFrame to check
            
        Returns:
            True if DataFrame is empty
        """
        try:
            return df.shape[0] == 0
        except Exception:
            # Fallback to shape check
            return len(df) == 0


# Configuration and logging utilities

def configure_dataframe_validation_logging(level: str = "INFO"):
    """
    Configure logging for DataFrame validation
    
    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR)
    """
    logging.getLogger(__name__).setLevel(getattr(logging, level.upper()))


def get_dataframe_safety_recommendations() -> Dict[str, str]:
    """
    Get recommendations for safe DataFrame usage
    
    Returns:
        Dictionary of recommendations
    """
    return {
        "boolean_context": "Use df.empty or df.shape[0] > 0 instead of if df:",
        "serialization": "Convert DataFrames to dict/list before JSON serialization",
        "session_storage": "Never store raw DataFrames in Django sessions",
        "error_handling": "Always catch ValueError for DataFrame ambiguity errors",
        "performance": "Use shape-based checks for large DataFrames",
        "logging": "Log DataFrame conversions for debugging",
        "testing": "Include DataFrame safety tests in your test suite"
    } 