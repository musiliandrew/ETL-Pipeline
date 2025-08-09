"""
Advanced Data Quality Assessment and Validation
===============================================
Production-grade data quality monitoring with comprehensive metrics
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, asdict
from enum import Enum
import re
from datetime import datetime
import structlog


class ValidationSeverity(Enum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class DataType(Enum):
    INTEGER = "integer"
    FLOAT = "float"
    STRING = "string"
    DATE = "date"
    BOOLEAN = "boolean"
    EMAIL = "email"
    PHONE = "phone"


@dataclass
class ValidationRule:
    """Data validation rule definition"""
    name: str
    column: str
    rule_type: str
    parameters: Dict[str, Any]
    severity: ValidationSeverity
    description: str
    
    def to_dict(self) -> Dict[str, Any]:
        data = asdict(self)
        data['severity'] = self.severity.value
        return data


@dataclass
class ValidationResult:
    """Result of a data validation check"""
    rule_name: str
    column: str
    passed: bool
    severity: ValidationSeverity
    message: str
    failed_count: int = 0
    total_count: int = 0
    failed_percentage: float = 0.0
    sample_failed_values: List[Any] = None
    
    def __post_init__(self):
        if self.sample_failed_values is None:
            self.sample_failed_values = []
        
        if self.total_count > 0:
            self.failed_percentage = (self.failed_count / self.total_count) * 100
    
    def to_dict(self) -> Dict[str, Any]:
        data = asdict(self)
        data['severity'] = self.severity.value
        return data


@dataclass
class DataQualityReport:
    """Comprehensive data quality assessment report"""
    dataset_name: str
    total_records: int
    total_columns: int
    validation_results: List[ValidationResult]
    overall_quality_score: float
    timestamp: datetime
    
    # Summary Statistics
    null_percentages: Dict[str, float]
    duplicate_count: int
    duplicate_percentage: float
    
    # Data Profiling
    column_profiles: Dict[str, Dict[str, Any]]
    
    def __post_init__(self):
        if not hasattr(self, 'timestamp') or not self.timestamp:
            self.timestamp = datetime.now()
    
    def to_dict(self) -> Dict[str, Any]:
        data = asdict(self)
        data['timestamp'] = self.timestamp.isoformat()
        data['validation_results'] = [r.to_dict() for r in self.validation_results]
        return data


class DataQualityAssessor:
    """Advanced data quality assessment and validation"""
    
    def __init__(self):
        self.logger = structlog.get_logger()
        self.default_rules = self._create_default_rules()
    
    def _create_default_rules(self) -> List[ValidationRule]:
        """Create default validation rules"""
        return [
            ValidationRule(
                name="null_check",
                column="*",
                rule_type="null_percentage",
                parameters={"max_percentage": 10.0},
                severity=ValidationSeverity.WARNING,
                description="Check for excessive null values"
            ),
            ValidationRule(
                name="duplicate_check",
                column="*",
                rule_type="duplicate_percentage",
                parameters={"max_percentage": 5.0},
                severity=ValidationSeverity.WARNING,
                description="Check for duplicate records"
            ),
            ValidationRule(
                name="data_type_check",
                column="*",
                rule_type="data_type",
                parameters={},
                severity=ValidationSeverity.ERROR,
                description="Validate expected data types"
            ),
        ]
    
    def assess_data_quality(
        self, 
        df: pd.DataFrame, 
        dataset_name: str = "unknown",
        custom_rules: Optional[List[ValidationRule]] = None
    ) -> DataQualityReport:
        """Perform comprehensive data quality assessment"""
        
        self.logger.info(f"Starting data quality assessment for {dataset_name}")
        
        # Combine default and custom rules
        rules = self.default_rules.copy()
        if custom_rules:
            rules.extend(custom_rules)
        
        # Run validations
        validation_results = []
        for rule in rules:
            try:
                result = self._execute_validation_rule(df, rule)
                if result:
                    validation_results.append(result)
            except Exception as e:
                self.logger.error(f"Error executing rule {rule.name}: {e}")
        
        # Calculate overall quality score
        quality_score = self._calculate_quality_score(validation_results, df)
        
        # Generate data profiling
        column_profiles = self._generate_column_profiles(df)
        
        # Calculate summary statistics
        null_percentages = self._calculate_null_percentages(df)
        duplicate_count = df.duplicated().sum()
        duplicate_percentage = (duplicate_count / len(df)) * 100 if len(df) > 0 else 0
        
        report = DataQualityReport(
            dataset_name=dataset_name,
            total_records=len(df),
            total_columns=len(df.columns),
            validation_results=validation_results,
            overall_quality_score=quality_score,
            timestamp=datetime.now(),
            null_percentages=null_percentages,
            duplicate_count=duplicate_count,
            duplicate_percentage=duplicate_percentage,
            column_profiles=column_profiles
        )
        
        self.logger.info(
            f"Data quality assessment completed for {dataset_name}",
            quality_score=quality_score,
            total_issues=len([r for r in validation_results if not r.passed])
        )
        
        return report
    
    def _execute_validation_rule(self, df: pd.DataFrame, rule: ValidationRule) -> Optional[ValidationResult]:
        """Execute a single validation rule"""
        
        if rule.rule_type == "null_percentage":
            return self._check_null_percentage(df, rule)
        elif rule.rule_type == "duplicate_percentage":
            return self._check_duplicate_percentage(df, rule)
        elif rule.rule_type == "data_type":
            return self._check_data_types(df, rule)
        elif rule.rule_type == "range":
            return self._check_value_range(df, rule)
        elif rule.rule_type == "regex":
            return self._check_regex_pattern(df, rule)
        elif rule.rule_type == "unique":
            return self._check_uniqueness(df, rule)
        elif rule.rule_type == "completeness":
            return self._check_completeness(df, rule)
        else:
            self.logger.warning(f"Unknown rule type: {rule.rule_type}")
            return None
    
    def _check_null_percentage(self, df: pd.DataFrame, rule: ValidationRule) -> ValidationResult:
        """Check null percentage for columns"""
        if rule.column == "*":
            # Check all columns
            max_null_percentage = 0
            worst_column = ""
            
            for col in df.columns:
                null_pct = (df[col].isnull().sum() / len(df)) * 100
                if null_pct > max_null_percentage:
                    max_null_percentage = null_pct
                    worst_column = col
            
            max_allowed = rule.parameters.get("max_percentage", 10.0)
            passed = max_null_percentage <= max_allowed
            
            return ValidationResult(
                rule_name=rule.name,
                column="all_columns",
                passed=passed,
                severity=rule.severity,
                message=f"Maximum null percentage: {max_null_percentage:.2f}% in column '{worst_column}' (threshold: {max_allowed}%)",
                failed_count=int(max_null_percentage) if not passed else 0,
                total_count=100,
                sample_failed_values=[worst_column] if not passed else []
            )
        else:
            # Check specific column
            if rule.column not in df.columns:
                return ValidationResult(
                    rule_name=rule.name,
                    column=rule.column,
                    passed=False,
                    severity=ValidationSeverity.ERROR,
                    message=f"Column '{rule.column}' not found in dataset"
                )
            
            null_count = df[rule.column].isnull().sum()
            null_percentage = (null_count / len(df)) * 100
            max_allowed = rule.parameters.get("max_percentage", 10.0)
            passed = null_percentage <= max_allowed
            
            return ValidationResult(
                rule_name=rule.name,
                column=rule.column,
                passed=passed,
                severity=rule.severity,
                message=f"Null percentage: {null_percentage:.2f}% (threshold: {max_allowed}%)",
                failed_count=null_count,
                total_count=len(df),
                failed_percentage=null_percentage
            )
    
    def _check_duplicate_percentage(self, df: pd.DataFrame, rule: ValidationRule) -> ValidationResult:
        """Check duplicate record percentage"""
        duplicate_count = df.duplicated().sum()
        duplicate_percentage = (duplicate_count / len(df)) * 100 if len(df) > 0 else 0
        max_allowed = rule.parameters.get("max_percentage", 5.0)
        passed = duplicate_percentage <= max_allowed
        
        return ValidationResult(
            rule_name=rule.name,
            column="all_columns",
            passed=passed,
            severity=rule.severity,
            message=f"Duplicate percentage: {duplicate_percentage:.2f}% (threshold: {max_allowed}%)",
            failed_count=duplicate_count,
            total_count=len(df),
            failed_percentage=duplicate_percentage
        )
    
    def _check_data_types(self, df: pd.DataFrame, rule: ValidationRule) -> ValidationResult:
        """Check data types match expected types"""
        # This is a simplified check - in reality, you'd have expected schema
        type_issues = []
        
        for col in df.columns:
            # Basic type validation
            if col.lower() in ['id', 'user_id']:
                if not pd.api.types.is_numeric_dtype(df[col]) and not pd.api.types.is_string_dtype(df[col]):
                    type_issues.append(f"{col} should be numeric or string")
            elif col.lower() in ['age']:
                if not pd.api.types.is_numeric_dtype(df[col]):
                    type_issues.append(f"{col} should be numeric")
            elif col.lower() in ['date', 'created_at', 'updated_at']:
                if not pd.api.types.is_datetime64_any_dtype(df[col]):
                    type_issues.append(f"{col} should be datetime")
        
        passed = len(type_issues) == 0
        
        return ValidationResult(
            rule_name=rule.name,
            column="all_columns",
            passed=passed,
            severity=rule.severity,
            message=f"Data type issues: {'; '.join(type_issues)}" if type_issues else "All data types valid",
            failed_count=len(type_issues),
            total_count=len(df.columns),
            sample_failed_values=type_issues
        )
    
    def _check_value_range(self, df: pd.DataFrame, rule: ValidationRule) -> ValidationResult:
        """Check if values are within expected range"""
        column = rule.column
        if column not in df.columns:
            return ValidationResult(
                rule_name=rule.name,
                column=column,
                passed=False,
                severity=ValidationSeverity.ERROR,
                message=f"Column '{column}' not found"
            )
        
        min_val = rule.parameters.get("min_value")
        max_val = rule.parameters.get("max_value")
        
        out_of_range_mask = pd.Series([False] * len(df))
        
        if min_val is not None:
            out_of_range_mask |= df[column] < min_val
        if max_val is not None:
            out_of_range_mask |= df[column] > max_val
        
        failed_count = out_of_range_mask.sum()
        passed = failed_count == 0
        
        return ValidationResult(
            rule_name=rule.name,
            column=column,
            passed=passed,
            severity=rule.severity,
            message=f"Values out of range [{min_val}, {max_val}]: {failed_count}",
            failed_count=failed_count,
            total_count=len(df),
            sample_failed_values=df[column][out_of_range_mask].head(5).tolist()
        )
    
    def _calculate_quality_score(self, results: List[ValidationResult], df: pd.DataFrame) -> float:
        """Calculate overall data quality score (0-100)"""
        if not results:
            return 100.0
        
        # Weight by severity
        severity_weights = {
            ValidationSeverity.INFO: 0.1,
            ValidationSeverity.WARNING: 0.3,
            ValidationSeverity.ERROR: 0.7,
            ValidationSeverity.CRITICAL: 1.0
        }
        
        total_weight = 0
        penalty_score = 0
        
        for result in results:
            weight = severity_weights[result.severity]
            total_weight += weight
            
            if not result.passed:
                # Penalty based on failed percentage and severity
                penalty = (result.failed_percentage / 100.0) * weight
                penalty_score += penalty
        
        if total_weight == 0:
            return 100.0
        
        # Calculate score (0-100)
        quality_score = max(0, 100 - (penalty_score / total_weight) * 100)
        return round(quality_score, 2)
    
    def _generate_column_profiles(self, df: pd.DataFrame) -> Dict[str, Dict[str, Any]]:
        """Generate detailed profiles for each column"""
        profiles = {}
        
        for col in df.columns:
            profile = {
                'data_type': str(df[col].dtype),
                'null_count': int(df[col].isnull().sum()),
                'null_percentage': float((df[col].isnull().sum() / len(df)) * 100),
                'unique_count': int(df[col].nunique()),
                'unique_percentage': float((df[col].nunique() / len(df)) * 100),
            }
            
            # Numeric columns
            if pd.api.types.is_numeric_dtype(df[col]):
                profile.update({
                    'min': float(df[col].min()) if not df[col].empty else None,
                    'max': float(df[col].max()) if not df[col].empty else None,
                    'mean': float(df[col].mean()) if not df[col].empty else None,
                    'median': float(df[col].median()) if not df[col].empty else None,
                    'std': float(df[col].std()) if not df[col].empty else None,
                })
            
            # String columns
            elif pd.api.types.is_string_dtype(df[col]) or pd.api.types.is_object_dtype(df[col]):
                non_null_values = df[col].dropna()
                if not non_null_values.empty:
                    profile.update({
                        'avg_length': float(non_null_values.astype(str).str.len().mean()),
                        'min_length': int(non_null_values.astype(str).str.len().min()),
                        'max_length': int(non_null_values.astype(str).str.len().max()),
                        'top_values': non_null_values.value_counts().head(5).to_dict()
                    })
            
            profiles[col] = profile
        
        return profiles
    
    def _calculate_null_percentages(self, df: pd.DataFrame) -> Dict[str, float]:
        """Calculate null percentages for all columns"""
        return {
            col: float((df[col].isnull().sum() / len(df)) * 100)
            for col in df.columns
        }
    
    def create_custom_validation_rules(self, schema: Dict[str, Any]) -> List[ValidationRule]:
        """Create validation rules from schema definition"""
        rules = []
        
        for column, config in schema.items():
            # Null check
            if not config.get('nullable', True):
                rules.append(ValidationRule(
                    name=f"null_check_{column}",
                    column=column,
                    rule_type="null_percentage",
                    parameters={"max_percentage": 0.0},
                    severity=ValidationSeverity.ERROR,
                    description=f"Column {column} should not have null values"
                ))
            
            # Range check for numeric columns
            if config.get('type') in ['integer', 'float']:
                min_val = config.get('min_value')
                max_val = config.get('max_value')
                if min_val is not None or max_val is not None:
                    rules.append(ValidationRule(
                        name=f"range_check_{column}",
                        column=column,
                        rule_type="range",
                        parameters={"min_value": min_val, "max_value": max_val},
                        severity=ValidationSeverity.WARNING,
                        description=f"Column {column} values should be in range [{min_val}, {max_val}]"
                    ))
            
            # Uniqueness check
            if config.get('unique', False):
                rules.append(ValidationRule(
                    name=f"unique_check_{column}",
                    column=column,
                    rule_type="unique",
                    parameters={},
                    severity=ValidationSeverity.ERROR,
                    description=f"Column {column} values should be unique"
                ))
        
        return rules


# Global data quality assessor instance
data_quality_assessor = DataQualityAssessor()


def get_data_quality_assessor() -> DataQualityAssessor:
    """Get the global data quality assessor instance"""
    return data_quality_assessor