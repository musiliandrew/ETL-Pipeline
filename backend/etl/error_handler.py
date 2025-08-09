"""
Error Handling and Dead Letter Queue Module
===========================================

Handles malformed files, data quality issues, and provides retry mechanisms
for production ETL pipelines.
"""

import os
import json
import shutil
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class DeadLetterQueue:
    """
    Manages files that fail processing by moving them to a dead letter directory
    with detailed error information for later analysis.
    """
    
    def __init__(self, base_path: str = "data"):
        self.base_path = Path(base_path)
        self.deadletter_path = self.base_path / "deadletter"
        self.processed_path = self.base_path / "processed"
        
        # Create directories if they don't exist
        self.deadletter_path.mkdir(parents=True, exist_ok=True)
        self.processed_path.mkdir(parents=True, exist_ok=True)
        
    def quarantine_file(self, file_path: str, error_info: Dict[str, Any]) -> str:
        """
        Move malformed file to dead letter queue with error metadata
        
        Parameters:
        -----------
        file_path : str
            Path to the problematic file
        error_info : dict
            Error details including stage, error message, and timestamp
            
        Returns:
        --------
        str
            Path to quarantined file
        """
        source_file = Path(file_path)
        if not source_file.exists():
            logger.warning(f"File not found for quarantine: {file_path}")
            return ""
            
        # Generate timestamped filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        quarantine_filename = f"{timestamp}_{source_file.name}"
        quarantine_path = self.deadletter_path / quarantine_filename
        
        try:
            # Copy file to dead letter queue
            shutil.copy2(source_file, quarantine_path)
            
            # Create error metadata file
            error_metadata = {
                "original_file": str(source_file),
                "quarantined_at": datetime.now().isoformat(),
                "error_stage": error_info.get("stage", "unknown"),
                "error_message": error_info.get("error", "Unknown error"),
                "error_type": error_info.get("error_type", "GeneralError"),
                "file_size": source_file.stat().st_size,
                "retry_count": error_info.get("retry_count", 0)
            }
            
            metadata_path = quarantine_path.with_suffix(quarantine_path.suffix + ".error")
            with open(metadata_path, 'w') as f:
                json.dump(error_metadata, f, indent=2)
            
            logger.error(f"File quarantined: {quarantine_filename} - {error_info.get('error', 'Unknown error')}")
            return str(quarantine_path)
            
        except Exception as e:
            logger.error(f"Failed to quarantine file {file_path}: {str(e)}")
            return ""
    
    def archive_successful_file(self, file_path: str, processing_info: Dict[str, Any]) -> str:
        """
        Move successfully processed file to processed archive
        """
        source_file = Path(file_path)
        if not source_file.exists():
            return ""
            
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        archive_filename = f"{timestamp}_{source_file.name}"
        archive_path = self.processed_path / archive_filename
        
        try:
            shutil.move(str(source_file), str(archive_path))
            
            # Create processing metadata
            metadata = {
                "processed_at": datetime.now().isoformat(),
                "rows_processed": processing_info.get("rows_processed", 0),
                "execution_time": processing_info.get("execution_time", "unknown"),
                "pipeline_version": "1.0"
            }
            
            metadata_path = archive_path.with_suffix(archive_path.suffix + ".processed")
            with open(metadata_path, 'w') as f:
                json.dump(metadata, f, indent=2)
                
            logger.info(f"File archived: {archive_filename}")
            return str(archive_path)
            
        except Exception as e:
            logger.error(f"Failed to archive file {file_path}: {str(e)}")
            return ""
    
    def get_quarantined_files(self) -> list:
        """Get list of files in dead letter queue"""
        error_files = list(self.deadletter_path.glob("*.error"))
        quarantined_files = []
        
        for error_file in error_files:
            try:
                with open(error_file, 'r') as f:
                    metadata = json.load(f)
                quarantined_files.append(metadata)
            except Exception as e:
                logger.error(f"Failed to read error metadata {error_file}: {str(e)}")
                
        return quarantined_files


class RetryHandler:
    """
    Handles retry logic for failed ETL operations
    """
    
    def __init__(self, max_retries: int = 3, retry_delay: float = 1.0):
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        
    def retry_with_backoff(self, func, *args, **kwargs):
        """
        Retry function with exponential backoff
        """
        import time
        
        for attempt in range(self.max_retries + 1):
            try:
                result = func(*args, **kwargs)
                if attempt > 0:
                    logger.info(f"Success after {attempt} retries")
                return result
                
            except Exception as e:
                if attempt == self.max_retries:
                    logger.error(f"Failed after {self.max_retries} retries: {str(e)}")
                    raise e
                
                wait_time = self.retry_delay * (2 ** attempt)  # Exponential backoff
                logger.warning(f"Attempt {attempt + 1} failed: {str(e)}. Retrying in {wait_time}s...")
                time.sleep(wait_time)


class DataQualityValidator:
    """
    Validates data quality and identifies issues before processing
    """
    
    def __init__(self):
        self.validation_rules = {
            'required_columns': ['user_id', 'age'],  # Minimum required columns
            'max_file_size_mb': 100,  # Maximum file size in MB
            'max_rows': 1000000,  # Maximum number of rows
            'age_range': (0, 150),  # Valid age range
        }
    
    def validate_file(self, file_path: str) -> Dict[str, Any]:
        """
        Validate file before processing
        
        Returns:
        --------
        dict
            Validation results with issues found
        """
        file_path = Path(file_path)
        validation_result = {
            'is_valid': True,
            'issues': [],
            'warnings': [],
            'file_info': {}
        }
        
        try:
            # Check file existence
            if not file_path.exists():
                validation_result['is_valid'] = False
                validation_result['issues'].append("File does not exist")
                return validation_result
            
            # Check file size
            file_size_mb = file_path.stat().st_size / (1024 * 1024)
            validation_result['file_info']['size_mb'] = round(file_size_mb, 2)
            
            if file_size_mb > self.validation_rules['max_file_size_mb']:
                validation_result['is_valid'] = False
                validation_result['issues'].append(f"File too large: {file_size_mb:.2f}MB > {self.validation_rules['max_file_size_mb']}MB")
            
            # Basic CSV validation
            if file_path.suffix.lower() == '.csv':
                import pandas as pd
                try:
                    # Try to read just the header
                    df_sample = pd.read_csv(file_path, nrows=5)
                    validation_result['file_info']['columns'] = list(df_sample.columns)
                    validation_result['file_info']['sample_rows'] = len(df_sample)
                    
                    # Check required columns
                    missing_required = [col for col in self.validation_rules['required_columns'] 
                                      if col not in df_sample.columns and 
                                         col.replace('_', '') not in [c.replace('_', '') for c in df_sample.columns]]
                    
                    if missing_required:
                        validation_result['warnings'].append(f"Missing recommended columns: {missing_required}")
                    
                    # Quick row count estimate
                    try:
                        total_rows = sum(1 for _ in open(file_path)) - 1  # Subtract header
                        validation_result['file_info']['estimated_rows'] = total_rows
                        
                        if total_rows > self.validation_rules['max_rows']:
                            validation_result['warnings'].append(f"Large file: {total_rows} rows")
                    except:
                        pass
                        
                except pd.errors.ParserError:
                    validation_result['is_valid'] = False
                    validation_result['issues'].append("CSV parsing error - malformed CSV")
                except pd.errors.EmptyDataError:
                    validation_result['is_valid'] = False
                    validation_result['issues'].append("Empty CSV file")
                except Exception as e:
                    validation_result['warnings'].append(f"CSV validation warning: {str(e)}")
            
            return validation_result
            
        except Exception as e:
            validation_result['is_valid'] = False
            validation_result['issues'].append(f"Validation error: {str(e)}")
            return validation_result
    
    def validate_data_quality(self, df) -> Dict[str, Any]:
        """
        Validate data quality of DataFrame
        """
        quality_report = {
            'total_rows': len(df),
            'issues': [],
            'warnings': [],
            'statistics': {}
        }
        
        try:
            # Check for completely empty DataFrame
            if len(df) == 0:
                quality_report['issues'].append("Empty dataset")
                return quality_report
            
            # Check for missing data
            missing_data = df.isnull().sum()
            if missing_data.sum() > 0:
                quality_report['statistics']['missing_values'] = missing_data.to_dict()
                high_missing = missing_data[missing_data > len(df) * 0.5]  # > 50% missing
                if len(high_missing) > 0:
                    quality_report['warnings'].append(f"High missing data in columns: {list(high_missing.index)}")
            
            # Age validation if age column exists
            if 'age' in df.columns:
                age_stats = {
                    'min': df['age'].min(),
                    'max': df['age'].max(),
                    'mean': df['age'].mean()
                }
                quality_report['statistics']['age'] = age_stats
                
                # Check age range
                min_age, max_age = self.validation_rules['age_range']
                invalid_ages = df[(df['age'] < min_age) | (df['age'] > max_age)]
                if len(invalid_ages) > 0:
                    quality_report['warnings'].append(f"Invalid ages found: {len(invalid_ages)} records")
            
            # Check for duplicate user_ids
            if 'user_id' in df.columns:
                duplicates = df['user_id'].duplicated().sum()
                if duplicates > 0:
                    quality_report['warnings'].append(f"Duplicate user_ids found: {duplicates} records")
            
            return quality_report
            
        except Exception as e:
            quality_report['issues'].append(f"Quality validation error: {str(e)}")
            return quality_report


# Example usage and testing
if __name__ == "__main__":
    print("Error Handling & Dead Letter Queue - Test")
    print("=" * 50)
    
    # Initialize components
    dlq = DeadLetterQueue()
    validator = DataQualityValidator()
    retry_handler = RetryHandler(max_retries=2)
    
    # Test file validation
    test_file = "data/input/users_clean.csv"
    if Path(test_file).exists():
        print(f"\n📋 Validating file: {test_file}")
        validation_result = validator.validate_file(test_file)
        
        print(f"✅ Valid: {validation_result['is_valid']}")
        if validation_result['issues']:
            print(f"❌ Issues: {validation_result['issues']}")
        if validation_result['warnings']:
            print(f"⚠️  Warnings: {validation_result['warnings']}")
        print(f"📊 File Info: {validation_result['file_info']}")
    
    # Show quarantined files
    quarantined = dlq.get_quarantined_files()
    print(f"\n📁 Quarantined files: {len(quarantined)}")
    for file_info in quarantined[:3]:  # Show first 3
        print(f"   - {file_info.get('original_file', 'unknown')} ({file_info.get('error_stage', 'unknown')})")