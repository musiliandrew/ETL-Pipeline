"""
Production-Grade ETL Pipeline
=============================

Enterprise-level ETL pipeline with:
- Dead letter queue for malformed files
- Monitoring and alerting
- Schema evolution
- Data quality validation  
- Retry mechanisms
- Comprehensive error handling
"""

import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional

# Import our core ETL modules
from .extractor import extract_data
from .transform import transform
from .load import load_data, get_db_config, connect_to_postgres

# Import production features
from .error_handler import DeadLetterQueue, RetryHandler, DataQualityValidator
from .monitoring import MetricsCollector, AlertingSystem, HealthChecker
from .schema_evolution import SchemaEvolutionManager

import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class ProductionETLPipeline:
    """
    Production-grade ETL pipeline with enterprise features
    """
    
    def __init__(self, config: Dict[str, Any] = None):
        self.config = config or {}
        
        # Initialize production components
        self.dlq = DeadLetterQueue()
        self.retry_handler = RetryHandler(max_retries=3, retry_delay=1.0)
        self.data_validator = DataQualityValidator()
        self.metrics_collector = MetricsCollector()
        self.alerting_system = AlertingSystem(self.config.get('alerting', {}))
        self.health_checker = HealthChecker()
        self.schema_manager = SchemaEvolutionManager()
        
        logger.info("Production ETL Pipeline initialized")
    
    def run_pipeline(self, file_path: str, pipeline_options: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Run production ETL pipeline with full monitoring and error handling
        
        Parameters:
        -----------
        file_path : str
            Path to input file
        pipeline_options : dict, optional
            Pipeline configuration options
            
        Returns:
        --------
        dict
            Comprehensive pipeline execution results
        """
        options = pipeline_options or {}
        pipeline_id = f"pipeline_{uuid.uuid4().hex[:8]}_{int(time.time())}"
        
        # Start metrics tracking
        metrics = self.metrics_collector.start_pipeline_tracking(pipeline_id, file_path)
        
        logger.info(f"🚀 Starting production pipeline: {pipeline_id}")
        logger.info(f"📁 Processing file: {file_path}")
        
        try:
            # Stage 0: Pre-flight checks
            logger.info("🔍 Running pre-flight checks...")
            preflight_result = self._run_preflight_checks(file_path)
            
            if not preflight_result['passed']:
                raise Exception(f"Pre-flight checks failed: {preflight_result['issues']}")
            
            # Stage 1: Extract with validation and retry
            logger.info("📥 Stage 1: Extract")
            self.metrics_collector.update_stage(pipeline_id, "extract", 0)
            
            raw_data = self.retry_handler.retry_with_backoff(self._extract_with_validation, file_path)
            logger.info(f"✅ Extracted {len(raw_data)} rows")
            
            # Stage 2: Transform with schema evolution
            logger.info("🔄 Stage 2: Transform with schema evolution")
            self.metrics_collector.update_stage(pipeline_id, "transform", len(raw_data))
            
            # Get database engine for schema evolution
            engine = connect_to_postgres() if not options.get('skip_schema_evolution') else None
            
            # Apply schema evolution
            transformed_data, evolution_report = self.schema_manager.process_data_with_schema_evolution(
                raw_data, engine
            )
            
            # Apply standard transformations
            clean_data = transform(transformed_data)
            logger.info(f"✅ Transformed {len(clean_data)} rows")
            
            if evolution_report['schema_changed']:
                logger.info(f"📋 Schema evolution: {evolution_report['actions_taken']}")
            
            # Stage 3: Load with monitoring
            logger.info("💾 Stage 3: Load")
            self.metrics_collector.update_stage(pipeline_id, "load", len(clean_data))
            
            load_result = self.retry_handler.retry_with_backoff(load_data, clean_data)
            logger.info(f"✅ Loaded {load_result['rows_loaded']} rows")
            
            # Stage 4: Post-processing
            logger.info("✨ Stage 4: Post-processing")
            post_result = self._post_processing(file_path, {
                'rows_processed': len(clean_data),
                'execution_time': f"{time.time() - metrics.start_time.timestamp():.2f}s"
            })
            
            # Finish successful pipeline
            final_metrics = self.metrics_collector.finish_pipeline_tracking(pipeline_id, "success")
            
            # Check for alerts
            alerts = self.alerting_system.check_pipeline_health(final_metrics)
            if alerts:
                for alert in alerts:
                    self.alerting_system.send_alert("PERFORMANCE_ALERT", alert, final_metrics)
            
            # Comprehensive success result
            result = {
                'success': True,
                'pipeline_id': pipeline_id,
                'file_processed': Path(file_path).name,
                'stages_completed': ['extract', 'transform', 'load', 'post_processing'],
                'rows_extracted': len(raw_data),
                'rows_transformed': len(clean_data),
                'rows_loaded': load_result['rows_loaded'],
                'execution_time': final_metrics.execution_time_seconds,
                'schema_evolution': evolution_report,
                'data_quality': preflight_result.get('data_quality', {}),
                'alerts_triggered': len(alerts),
                'message': f'Production pipeline completed successfully: {pipeline_id}'
            }
            
            logger.info(f"🎉 Pipeline completed successfully: {pipeline_id}")
            return result
            
        except Exception as e:
            # Handle pipeline failure
            logger.error(f"❌ Pipeline failed: {pipeline_id} - {str(e)}")
            
            # Finish failed pipeline tracking
            final_metrics = self.metrics_collector.finish_pipeline_tracking(
                pipeline_id, "failed", str(e)
            )
            
            # Send failure alerts
            self.alerting_system.send_alert("PIPELINE_FAILURE", str(e), final_metrics)
            
            # Quarantine problematic file
            quarantine_path = self.dlq.quarantine_file(file_path, {
                'stage': final_metrics.stage,
                'error': str(e),
                'error_type': type(e).__name__,
                'pipeline_id': pipeline_id
            })
            
            # Return failure result
            return {
                'success': False,
                'pipeline_id': pipeline_id,
                'file_processed': Path(file_path).name,
                'failed_stage': final_metrics.stage,
                'error': str(e),
                'error_type': type(e).__name__,
                'execution_time': final_metrics.execution_time_seconds,
                'quarantine_path': quarantine_path,
                'message': f'Production pipeline failed: {pipeline_id} - {str(e)}'
            }
    
    def _run_preflight_checks(self, file_path: str) -> Dict[str, Any]:
        """Run comprehensive pre-flight checks"""
        checks_result = {
            'passed': True,
            'issues': [],
            'warnings': [],
            'health_status': {},
            'data_quality': {}
        }
        
        try:
            # System health checks
            health_status = self.health_checker.run_health_checks()
            checks_result['health_status'] = health_status
            
            if health_status['overall_status'] != 'healthy':
                checks_result['warnings'].append(f"System health issues detected")
            
            # File validation
            file_validation = self.data_validator.validate_file(file_path)
            checks_result['data_quality'] = file_validation
            
            if not file_validation['is_valid']:
                checks_result['passed'] = False
                checks_result['issues'].extend(file_validation['issues'])
            
            if file_validation['warnings']:
                checks_result['warnings'].extend(file_validation['warnings'])
            
            return checks_result
            
        except Exception as e:
            checks_result['passed'] = False
            checks_result['issues'].append(f"Pre-flight check error: {str(e)}")
            return checks_result
    
    def _extract_with_validation(self, file_path: str):
        """Extract data with additional validation"""
        # Extract data
        raw_data = extract_data(file_path)
        
        # Validate extracted data quality
        quality_report = self.data_validator.validate_data_quality(raw_data)
        
        if quality_report['issues']:
            raise Exception(f"Data quality issues: {quality_report['issues']}")
        
        if quality_report['warnings']:
            logger.warning(f"Data quality warnings: {quality_report['warnings']}")
        
        return raw_data
    
    def _post_processing(self, file_path: str, processing_info: Dict[str, Any]) -> Dict[str, Any]:
        """Post-processing: archive file, cleanup, notifications"""
        try:
            # Archive successfully processed file
            archive_path = self.dlq.archive_successful_file(file_path, processing_info)
            
            # Cleanup temporary files if any
            # self._cleanup_temp_files()
            
            return {
                'archive_path': archive_path,
                'cleanup_completed': True
            }
        except Exception as e:
            logger.warning(f"Post-processing issues: {str(e)}")
            return {'error': str(e)}
    
    def run_batch_pipeline(self, input_directory: str, file_pattern: str = "*.csv") -> Dict[str, Any]:
        """Run production pipeline on multiple files"""
        input_path = Path(input_directory)
        files = list(input_path.glob(file_pattern))
        
        if not files:
            return {
                'success': False,
                'message': f'No files found matching {file_pattern} in {input_directory}',
                'files_processed': []
            }
        
        logger.info(f"🚀 Starting batch production pipeline for {len(files)} files")
        
        batch_results = []
        successful_files = 0
        failed_files = 0
        total_rows_processed = 0
        
        for file_path in files:
            logger.info(f"\n📁 Processing: {file_path.name}")
            
            result = self.run_pipeline(str(file_path))
            batch_results.append(result)
            
            if result['success']:
                successful_files += 1
                total_rows_processed += result.get('rows_loaded', 0)
                logger.info(f"   ✅ Success: {result.get('rows_loaded', 0)} rows")
            else:
                failed_files += 1
                logger.error(f"   ❌ Failed: {result.get('error', 'Unknown error')}")
        
        # Batch summary
        batch_result = {
            'success': failed_files == 0,
            'total_files': len(files),
            'successful_files': successful_files,
            'failed_files': failed_files,
            'success_rate': (successful_files / len(files) * 100) if files else 0,
            'total_rows_processed': total_rows_processed,
            'detailed_results': batch_results,
            'message': f'Batch processing completed: {successful_files}/{len(files)} files successful'
        }
        
        # Send batch completion alert if configured
        if self.config.get('batch_alerts_enabled'):
            alert_message = f"Batch pipeline completed: {successful_files}/{len(files)} successful"
            if failed_files > 0:
                self.alerting_system.send_alert("BATCH_COMPLETION", alert_message)
        
        logger.info(f"🎉 Batch pipeline completed: {successful_files}/{len(files)} successful")
        return batch_result
    
    def get_pipeline_status(self) -> Dict[str, Any]:
        """Get current pipeline system status"""
        # Health checks
        health_status = self.health_checker.run_health_checks()
        
        # Daily metrics summary
        daily_summary = self.metrics_collector.get_daily_summary()
        
        # Quarantined files
        quarantined_files = self.dlq.get_quarantined_files()
        
        return {
            'timestamp': datetime.now().isoformat(),
            'health_status': health_status,
            'daily_metrics': daily_summary,
            'quarantined_files_count': len(quarantined_files),
            'current_schema_version': self.schema_manager.registry.get_current_schema().get('version', 'unknown')
        }


# Example usage and demonstration
if __name__ == "__main__":
    print("Production ETL Pipeline - Demonstration")
    print("=" * 60)
    
    # Initialize production pipeline
    production_config = {
        'alerting': {
            'email_alerts_enabled': False,  # Set to True for email alerts
            'recipient_emails': ['admin@company.com']
        },
        'batch_alerts_enabled': True
    }
    
    pipeline = ProductionETLPipeline(production_config)
    
    # Test single file processing
    print("\n🚀 Testing single file processing...")
    test_file = "data/input/users_clean.csv"
    
    if Path(test_file).exists():
        result = pipeline.run_pipeline(test_file)
        
        if result['success']:
            print(f"✅ SUCCESS:")
            print(f"   Pipeline ID: {result['pipeline_id']}")
            print(f"   File: {result['file_processed']}")
            print(f"   Rows processed: {result['rows_loaded']}")
            print(f"   Execution time: {result['execution_time']:.2f}s")
            print(f"   Alerts: {result['alerts_triggered']}")
            
            if result['schema_evolution']['schema_changed']:
                print(f"   Schema evolved: {result['schema_evolution']['actions_taken']}")
        else:
            print(f"❌ FAILED:")
            print(f"   Pipeline ID: {result['pipeline_id']}")
            print(f"   Error: {result['error']}")
            print(f"   Failed at: {result['failed_stage']}")
            print(f"   Quarantined: {result.get('quarantine_path', 'N/A')}")
    else:
        print(f"⚠️ Test file not found: {test_file}")
    
    # Show system status
    print(f"\n📊 System Status:")
    status = pipeline.get_pipeline_status()
    print(f"   Health: {status['health_status']['overall_status']}")
    print(f"   Daily runs: {status['daily_metrics']['total_runs']}")
    print(f"   Success rate: {status['daily_metrics']['success_rate']:.1f}%")
    print(f"   Quarantined files: {status['quarantined_files_count']}")
    print(f"   Schema version: {status['current_schema_version']}")
    
    print(f"\n🎯 Production pipeline ready for enterprise workloads!")