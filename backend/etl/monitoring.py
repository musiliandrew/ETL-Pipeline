"""
ETL Monitoring and Alerting System
==================================

Provides comprehensive monitoring, metrics collection, and alerting 
for production ETL pipelines.
"""

import json
import time
import smtplib
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, asdict
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


@dataclass
class PipelineMetrics:
    """Data class for pipeline execution metrics"""
    pipeline_id: str
    start_time: datetime
    end_time: Optional[datetime] = None
    status: str = "running"  # running, success, failed
    stage: str = "extract"   # extract, transform, load
    file_path: str = ""
    rows_processed: int = 0
    error_message: str = ""
    execution_time_seconds: float = 0.0
    data_quality_score: float = 100.0
    memory_usage_mb: float = 0.0
    
    def to_dict(self) -> Dict[str, Any]:
        result = asdict(self)
        # Convert datetime objects to ISO strings
        if self.start_time:
            result['start_time'] = self.start_time.isoformat()
        if self.end_time:
            result['end_time'] = self.end_time.isoformat()
        return result


class MetricsCollector:
    """
    Collects and stores pipeline execution metrics
    """
    
    def __init__(self, metrics_dir: str = "logs/metrics"):
        self.metrics_dir = Path(metrics_dir)
        self.metrics_dir.mkdir(parents=True, exist_ok=True)
        self.current_metrics: Dict[str, PipelineMetrics] = {}
        
    def start_pipeline_tracking(self, pipeline_id: str, file_path: str = "") -> PipelineMetrics:
        """Start tracking a pipeline execution"""
        metrics = PipelineMetrics(
            pipeline_id=pipeline_id,
            start_time=datetime.now(),
            file_path=file_path
        )
        self.current_metrics[pipeline_id] = metrics
        logger.info(f"Started tracking pipeline: {pipeline_id}")
        return metrics
    
    def update_stage(self, pipeline_id: str, stage: str, rows_processed: int = 0):
        """Update pipeline stage"""
        if pipeline_id in self.current_metrics:
            self.current_metrics[pipeline_id].stage = stage
            self.current_metrics[pipeline_id].rows_processed = rows_processed
            logger.info(f"Pipeline {pipeline_id} - Stage: {stage}, Rows: {rows_processed}")
    
    def finish_pipeline_tracking(self, pipeline_id: str, status: str, error_message: str = ""):
        """Finish tracking and save metrics"""
        if pipeline_id not in self.current_metrics:
            logger.warning(f"Pipeline {pipeline_id} not found in tracking")
            return
            
        metrics = self.current_metrics[pipeline_id]
        metrics.end_time = datetime.now()
        metrics.status = status
        metrics.error_message = error_message
        metrics.execution_time_seconds = (metrics.end_time - metrics.start_time).total_seconds()
        
        # Save metrics to file
        self._save_metrics(metrics)
        
        # Remove from current tracking
        del self.current_metrics[pipeline_id]
        
        logger.info(f"Finished tracking pipeline: {pipeline_id} - Status: {status}")
        return metrics
    
    def _save_metrics(self, metrics: PipelineMetrics):
        """Save metrics to JSON file"""
        try:
            date_str = metrics.start_time.strftime("%Y%m%d")
            metrics_file = self.metrics_dir / f"pipeline_metrics_{date_str}.json"
            
            # Load existing metrics or create new list
            if metrics_file.exists():
                with open(metrics_file, 'r') as f:
                    existing_metrics = json.load(f)
            else:
                existing_metrics = []
            
            # Add new metrics
            existing_metrics.append(metrics.to_dict())
            
            # Save updated metrics
            with open(metrics_file, 'w') as f:
                json.dump(existing_metrics, f, indent=2)
                
        except Exception as e:
            logger.error(f"Failed to save metrics: {str(e)}")
    
    def get_daily_summary(self, date: datetime = None) -> Dict[str, Any]:
        """Get summary statistics for a day"""
        if date is None:
            date = datetime.now()
            
        date_str = date.strftime("%Y%m%d")
        metrics_file = self.metrics_dir / f"pipeline_metrics_{date_str}.json"
        
        if not metrics_file.exists():
            return {"date": date_str, "total_runs": 0, "success_rate": 0}
        
        try:
            with open(metrics_file, 'r') as f:
                daily_metrics = json.load(f)
            
            total_runs = len(daily_metrics)
            successful_runs = sum(1 for m in daily_metrics if m['status'] == 'success')
            failed_runs = total_runs - successful_runs
            
            total_rows = sum(m.get('rows_processed', 0) for m in daily_metrics)
            avg_execution_time = sum(m.get('execution_time_seconds', 0) for m in daily_metrics) / total_runs if total_runs > 0 else 0
            
            return {
                "date": date_str,
                "total_runs": total_runs,
                "successful_runs": successful_runs,
                "failed_runs": failed_runs,
                "success_rate": (successful_runs / total_runs * 100) if total_runs > 0 else 0,
                "total_rows_processed": total_rows,
                "average_execution_time": round(avg_execution_time, 2),
                "failed_pipelines": [m for m in daily_metrics if m['status'] == 'failed']
            }
            
        except Exception as e:
            logger.error(f"Failed to generate daily summary: {str(e)}")
            return {"date": date_str, "error": str(e)}


class AlertingSystem:
    """
    Handles alerts for pipeline failures and anomalies
    """
    
    def __init__(self, config: Dict[str, Any] = None):
        self.config = config or {}
        self.alert_thresholds = {
            'max_execution_time': 300,  # 5 minutes
            'min_success_rate': 90,     # 90%
            'max_failed_runs': 3,       # consecutive failures
            'data_quality_threshold': 80  # minimum data quality score
        }
        self.consecutive_failures = 0
        
    def check_pipeline_health(self, metrics: PipelineMetrics) -> List[str]:
        """Check pipeline health and return list of alerts"""
        alerts = []
        
        # Check execution time
        if metrics.execution_time_seconds > self.alert_thresholds['max_execution_time']:
            alerts.append(f"SLOW_EXECUTION: Pipeline took {metrics.execution_time_seconds:.2f}s (threshold: {self.alert_thresholds['max_execution_time']}s)")
        
        # Check failure status
        if metrics.status == 'failed':
            self.consecutive_failures += 1
            alerts.append(f"PIPELINE_FAILURE: {metrics.error_message}")
            
            if self.consecutive_failures >= self.alert_thresholds['max_failed_runs']:
                alerts.append(f"CRITICAL: {self.consecutive_failures} consecutive failures")
        else:
            self.consecutive_failures = 0
        
        # Check data quality
        if metrics.data_quality_score < self.alert_thresholds['data_quality_threshold']:
            alerts.append(f"DATA_QUALITY_ISSUE: Score {metrics.data_quality_score:.1f}% (threshold: {self.alert_thresholds['data_quality_threshold']}%)")
        
        return alerts
    
    def send_alert(self, alert_type: str, message: str, pipeline_metrics: PipelineMetrics = None):
        """Send alert notification"""
        alert_message = f"[ETL ALERT - {alert_type}] {message}"
        
        if pipeline_metrics:
            alert_message += f"""
            
Pipeline Details:
- Pipeline ID: {pipeline_metrics.pipeline_id}
- File: {pipeline_metrics.file_path}
- Stage: {pipeline_metrics.stage}
- Status: {pipeline_metrics.status}
- Execution Time: {pipeline_metrics.execution_time_seconds:.2f}s
- Rows Processed: {pipeline_metrics.rows_processed}
- Timestamp: {pipeline_metrics.start_time}
"""
        
        # Log alert
        logger.error(alert_message)
        
        # Send email if configured
        if self.config.get('email_alerts_enabled'):
            self._send_email_alert(alert_type, alert_message)
        
        # Write to alert file
        self._write_alert_file(alert_type, alert_message, pipeline_metrics)
    
    def _send_email_alert(self, alert_type: str, message: str):
        """Send email alert (requires email configuration)"""
        try:
            # Email configuration would come from environment variables
            smtp_server = self.config.get('smtp_server', 'localhost')
            smtp_port = self.config.get('smtp_port', 587)
            sender_email = self.config.get('sender_email', 'etl-alerts@company.com')
            recipient_emails = self.config.get('recipient_emails', [])
            
            if not recipient_emails:
                logger.warning("No recipient emails configured for alerts")
                return
            
            # Simple email without MIME (for basic functionality)
            email_body = f"""
Subject: ETL Pipeline Alert: {alert_type}
From: {sender_email}
To: {', '.join(recipient_emails)}

{message}
"""
            
            # Note: In production, you'd configure SMTP credentials and use proper MIME
            # with smtplib.SMTP(smtp_server, smtp_port) as server:
            #     server.starttls()
            #     server.login(sender_email, password)
            #     server.sendmail(sender_email, recipient_emails, email_body)
            
            logger.info(f"Email alert prepared: {alert_type}")
            
        except Exception as e:
            logger.error(f"Failed to send email alert: {str(e)}")
    
    def _write_alert_file(self, alert_type: str, message: str, pipeline_metrics: PipelineMetrics = None):
        """Write alert to file for monitoring systems to pick up"""
        try:
            alerts_dir = Path("logs/alerts")
            alerts_dir.mkdir(parents=True, exist_ok=True)
            
            alert_data = {
                "timestamp": datetime.now().isoformat(),
                "alert_type": alert_type,
                "message": message,
                "pipeline_metrics": pipeline_metrics.to_dict() if pipeline_metrics else None
            }
            
            alert_file = alerts_dir / f"alert_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            with open(alert_file, 'w') as f:
                json.dump(alert_data, f, indent=2)
                
        except Exception as e:
            logger.error(f"Failed to write alert file: {str(e)}")


class HealthChecker:
    """
    Monitors overall system health and dependencies
    """
    
    def __init__(self):
        self.health_checks = {
            'database_connection': self._check_database,
            'disk_space': self._check_disk_space,
            'memory_usage': self._check_memory,
            'input_directory': self._check_input_directory
        }
    
    def run_health_checks(self) -> Dict[str, Any]:
        """Run all health checks"""
        health_status = {
            "timestamp": datetime.now().isoformat(),
            "overall_status": "healthy",
            "checks": {}
        }
        
        for check_name, check_func in self.health_checks.items():
            try:
                result = check_func()
                health_status["checks"][check_name] = result
                
                if not result.get("status") == "ok":
                    health_status["overall_status"] = "unhealthy"
                    
            except Exception as e:
                health_status["checks"][check_name] = {
                    "status": "error",
                    "message": str(e)
                }
                health_status["overall_status"] = "unhealthy"
        
        return health_status
    
    def _check_database(self) -> Dict[str, Any]:
        """Check database connectivity"""
        try:
            from load import connect_to_postgres
            engine = connect_to_postgres()
            with engine.connect() as conn:
                conn.execute("SELECT 1")
            return {"status": "ok", "message": "Database connection successful"}
        except Exception as e:
            return {"status": "error", "message": f"Database connection failed: {str(e)}"}
    
    def _check_disk_space(self) -> Dict[str, Any]:
        """Check available disk space"""
        try:
            import shutil
            total, used, free = shutil.disk_usage(".")
            free_gb = free / (1024**3)
            usage_percent = (used / total) * 100
            
            if free_gb < 1:  # Less than 1GB free
                return {"status": "warning", "message": f"Low disk space: {free_gb:.2f}GB free"}
            elif usage_percent > 90:
                return {"status": "warning", "message": f"High disk usage: {usage_percent:.1f}%"}
            else:
                return {"status": "ok", "message": f"Disk space OK: {free_gb:.2f}GB free"}
        except Exception as e:
            return {"status": "error", "message": f"Disk check failed: {str(e)}"}
    
    def _check_memory(self) -> Dict[str, Any]:
        """Check memory usage"""
        try:
            import psutil
            memory = psutil.virtual_memory()
            if memory.percent > 90:
                return {"status": "warning", "message": f"High memory usage: {memory.percent:.1f}%"}
            else:
                return {"status": "ok", "message": f"Memory usage OK: {memory.percent:.1f}%"}
        except ImportError:
            return {"status": "info", "message": "psutil not installed - memory check skipped"}
        except Exception as e:
            return {"status": "error", "message": f"Memory check failed: {str(e)}"}
    
    def _check_input_directory(self) -> Dict[str, Any]:
        """Check input directory accessibility"""
        try:
            input_dir = Path("data/input")
            if not input_dir.exists():
                return {"status": "error", "message": "Input directory does not exist"}
            
            # Count files waiting to be processed
            csv_files = list(input_dir.glob("*.csv"))
            json_files = list(input_dir.glob("*.json"))
            total_files = len(csv_files) + len(json_files)
            
            if total_files > 100:
                return {"status": "warning", "message": f"Large backlog: {total_files} files waiting"}
            else:
                return {"status": "ok", "message": f"Input directory OK: {total_files} files pending"}
        except Exception as e:
            return {"status": "error", "message": f"Input directory check failed: {str(e)}"}


# Example usage and testing
if __name__ == "__main__":
    print("ETL Monitoring & Alerting System - Test")
    print("=" * 50)
    
    # Initialize monitoring components
    metrics_collector = MetricsCollector()
    alerting_system = AlertingSystem()
    health_checker = HealthChecker()
    
    # Test metrics collection
    print("\n📊 Testing metrics collection...")
    pipeline_id = f"test_pipeline_{int(time.time())}"
    metrics = metrics_collector.start_pipeline_tracking(pipeline_id, "test_file.csv")
    
    # Simulate pipeline stages
    time.sleep(0.1)
    metrics_collector.update_stage(pipeline_id, "transform", 100)
    time.sleep(0.1)
    metrics_collector.update_stage(pipeline_id, "load", 100)
    
    # Finish tracking
    final_metrics = metrics_collector.finish_pipeline_tracking(pipeline_id, "success")
    print(f"✅ Pipeline tracked: {final_metrics.execution_time_seconds:.3f}s")
    
    # Test health checks
    print("\n🏥 Running health checks...")
    health_status = health_checker.run_health_checks()
    print(f"Overall health: {health_status['overall_status']}")
    
    for check_name, result in health_status['checks'].items():
        status_icon = "✅" if result['status'] == 'ok' else "⚠️" if result['status'] == 'warning' else "❌"
        print(f"   {status_icon} {check_name}: {result['message']}")
    
    # Test daily summary
    print("\n📈 Daily summary...")
    summary = metrics_collector.get_daily_summary()
    print(f"   Total runs today: {summary.get('total_runs', 0)}")
    print(f"   Success rate: {summary.get('success_rate', 0):.1f}%")
    print(f"   Average execution time: {summary.get('average_execution_time', 0)}s")