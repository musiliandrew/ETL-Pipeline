"""
ETL Pipeline Conductor
======================

This module orchestrates the complete ETL process by coordinating:
- extractor.py: Data extraction from CSV/JSON files
- transform.py: Data cleaning and standardization  
- load.py: Data loading into PostgreSQL database

Purpose: Enable analyst queries like "Show me active users over 30 who signed up last week"
"""

import time
import logging
from pathlib import Path

# Import our ETL modules
from .extractor import extract_data
from .transform import transform
from .load import load_data

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def get_current_stage(stages_completed):
    """Helper function to determine which stage failed"""
    stage_mapping = {
        0: 'extract',
        1: 'transform', 
        2: 'load'
    }
    return stage_mapping.get(len(stages_completed), 'unknown')


def run_etl_pipeline(file_path, verbose=True):
    """
    ETL Pipeline Conductor Function
    
    Orchestrates the complete ETL process by calling:
    1. extractor.extract_data() → raw DataFrame
    2. transform.transform() → clean DataFrame  
    3. load.load_data() → PostgreSQL database
    
    Parameters:
    -----------
    file_path : str
        Path to the CSV/JSON file to process
    verbose : bool, optional
        Print progress messages during execution (default: True)
        
    Returns:
    --------
    dict
        Pipeline execution results with success status, timing, and details
        
    Example:
    --------
    result = run_etl_pipeline('data/input/users_messy.csv')
    if result['success']:
        print(f"✅ Pipeline completed: {result['rows_processed']} rows loaded")
    """
    
    # Initialize pipeline tracking
    pipeline_start = time.time()
    stages_completed = []
    file_name = Path(file_path).name
    
    logger.info(f"🚀 Starting ETL pipeline for file: {file_name}")
    
    try:
        # Stage 1: Extract Data
        if verbose:
            print("🔄 Stage 1: Extracting data from file...")
        
        raw_data = extract_data(file_path)
        stages_completed.append('extract')
        
        if verbose:
            print(f"   ✅ Extracted {len(raw_data)} rows from {file_name}")
        logger.info(f"Extract stage completed: {len(raw_data)} rows extracted")
        
        # Stage 2: Transform Data
        if verbose:
            print("🔄 Stage 2: Transforming and cleaning data...")
            
        clean_data = transform(raw_data)
        stages_completed.append('transform')
        
        if verbose:
            print(f"   ✅ Transformed data: {len(clean_data)} clean rows ready")
        logger.info(f"Transform stage completed: {len(clean_data)} rows cleaned")
        
        # Stage 3: Load Data  
        if verbose:
            print("🔄 Stage 3: Loading data into PostgreSQL...")
            
        load_result = load_data(clean_data)  # Uses .env automatically
        stages_completed.append('load')
        
        if verbose:
            print(f"   ✅ Loaded {load_result['rows_loaded']} rows into database")
        logger.info(f"Load stage completed: {load_result['rows_loaded']} rows loaded")
        
        # Calculate execution time
        pipeline_time = time.time() - pipeline_start
        
        # Success result
        result = {
            'success': True,
            'stages_completed': stages_completed,
            'file_processed': file_name,
            'rows_extracted': len(raw_data),
            'rows_transformed': len(clean_data),
            'rows_loaded': load_result['rows_loaded'],
            'execution_time': f"{pipeline_time:.2f} seconds",
            'message': f'ETL pipeline completed successfully for {file_name}'
        }
        
        if verbose:
            print(f"🎉 Pipeline completed successfully in {result['execution_time']}")
        logger.info(f"Pipeline completed successfully: {result}")
        
        return result
        
    except Exception as e:
        # Calculate execution time even on failure
        pipeline_time = time.time() - pipeline_start
        failed_stage = get_current_stage(stages_completed)
        
        error_result = {
            'success': False,
            'stages_completed': stages_completed,
            'failed_stage': failed_stage,
            'file_processed': file_name,
            'error': str(e),
            'execution_time': f"{pipeline_time:.2f} seconds",
            'message': f'Pipeline failed at {failed_stage} stage: {str(e)}'
        }
        
        if verbose:
            print(f"❌ Pipeline failed at {failed_stage} stage: {str(e)}")
        logger.error(f"Pipeline failed: {error_result}")
        
        return error_result


def run_batch_pipeline(input_directory, file_pattern="*.csv", verbose=True):
    """
    Run ETL pipeline on multiple files in a directory
    
    Parameters:
    -----------
    input_directory : str
        Directory containing files to process
    file_pattern : str, optional
        File pattern to match (default: "*.csv")
    verbose : bool, optional
        Print progress messages (default: True)
        
    Returns:
    --------
    dict
        Batch processing results
    """
    input_path = Path(input_directory)
    files = list(input_path.glob(file_pattern))
    
    if not files:
        return {
            'success': False,
            'message': f'No files found matching {file_pattern} in {input_directory}',
            'files_processed': []
        }
    
    if verbose:
        print(f"🚀 Starting batch ETL pipeline for {len(files)} files")
    
    results = []
    successful_files = 0
    failed_files = 0
    
    for file_path in files:
        if verbose:
            print(f"\n📁 Processing file: {file_path.name}")
            
        result = run_etl_pipeline(str(file_path), verbose=False)  # Less verbose for batch
        results.append(result)
        
        if result['success']:
            successful_files += 1
            if verbose:
                print(f"   ✅ {file_path.name}: {result['rows_loaded']} rows loaded")
        else:
            failed_files += 1
            if verbose:
                print(f"   ❌ {file_path.name}: {result['message']}")
    
    batch_result = {
        'success': failed_files == 0,
        'total_files': len(files),
        'successful_files': successful_files,
        'failed_files': failed_files,
        'files_processed': results,
        'message': f'Batch processing completed: {successful_files}/{len(files)} files successful'
    }
    
    if verbose:
        print(f"\n🎉 Batch pipeline completed: {successful_files}/{len(files)} files successful")
    
    return batch_result


# Example usage and testing
if __name__ == "__main__":
    print("ETL Pipeline Conductor - Example Usage")
    print("=" * 50)
    
    # Example 1: Single file processing
    print("\n📋 Example 1: Single File Processing")
    sample_file = "data/input/users_messy.csv"
    
    try:
        result = run_etl_pipeline(sample_file)
        
        if result['success']:
            print(f"\n✅ SUCCESS SUMMARY:")
            print(f"   File: {result['file_processed']}")
            print(f"   Rows extracted: {result['rows_extracted']}")
            print(f"   Rows transformed: {result['rows_transformed']}")
            print(f"   Rows loaded: {result['rows_loaded']}")
            print(f"   Execution time: {result['execution_time']}")
            print(f"\n🎯 Ready for analyst query: 'Show me active users over 30 who signed up last week'")
        else:
            print(f"\n❌ PIPELINE FAILED:")
            print(f"   Failed at: {result['failed_stage']} stage")
            print(f"   Error: {result['error']}")
            print(f"   Stages completed: {result['stages_completed']}")
            
    except FileNotFoundError:
        print(f"⚠️  Sample file not found: {sample_file}")
        print("   Please ensure you have data files in the data/input/ directory")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
    
    # Example 2: Batch processing
    print(f"\n📋 Example 2: Batch File Processing")
    input_dir = "data/input"
    
    try:
        batch_result = run_batch_pipeline(input_dir)
        
        print(f"\n📊 BATCH PROCESSING SUMMARY:")
        print(f"   Total files: {batch_result['total_files']}")
        print(f"   Successful: {batch_result['successful_files']}")
        print(f"   Failed: {batch_result['failed_files']}")
        print(f"   Success rate: {batch_result['successful_files']/batch_result['total_files']*100:.1f}%")
        
    except Exception as e:
        print(f"❌ Batch processing error: {e}")
    
    print(f"\n🚀 Pipeline ready! Your database now supports analyst queries!")
    print(f"💡 Try: SELECT * FROM users WHERE is_active = true AND age > 30;")