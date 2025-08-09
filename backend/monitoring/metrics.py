"""
Advanced Metrics Collection and Monitoring
==========================================
Production-grade monitoring with Prometheus metrics and custom KPIs
"""

import time
import psutil
import asyncio
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
from prometheus_client import Counter, Histogram, Gauge, CollectorRegistry
import structlog
from enum import Enum


class MetricType(Enum):
    COUNTER = "counter"
    HISTOGRAM = "histogram"
    GAUGE = "gauge"


@dataclass
class PipelineMetrics:
    """Pipeline execution metrics"""
    pipeline_id: str
    start_time: datetime
    end_time: Optional[datetime] = None
    status: str = "running"
    
    # Processing Metrics
    records_processed: int = 0
    records_failed: int = 0
    records_skipped: int = 0
    
    # Performance Metrics
    processing_time_seconds: float = 0.0
    throughput_records_per_second: float = 0.0
    
    # Stage Metrics
    extract_time_seconds: float = 0.0
    transform_time_seconds: float = 0.0
    load_time_seconds: float = 0.0
    
    # Resource Metrics
    peak_memory_mb: float = 0.0
    cpu_usage_percent: float = 0.0
    
    # Data Quality Metrics
    data_quality_score: float = 0.0
    null_percentage: float = 0.0
    duplicate_percentage: float = 0.0
    
    # Error Metrics
    error_messages: List[str] = None
    
    def __post_init__(self):
        if self.error_messages is None:
            self.error_messages = []
    
    @property
    def duration_seconds(self) -> float:
        if self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return (datetime.now() - self.start_time).total_seconds()
    
    def to_dict(self) -> Dict[str, Any]:
        data = asdict(self)
        data['start_time'] = self.start_time.isoformat()
        if self.end_time:
            data['end_time'] = self.end_time.isoformat()
        data['duration_seconds'] = self.duration_seconds
        return data


class MetricsCollector:
    """Advanced metrics collection and monitoring"""
    
    def __init__(self):
        self.logger = structlog.get_logger()
        self.registry = CollectorRegistry()
        self.active_pipelines: Dict[str, PipelineMetrics] = {}
        self.pipeline_history: List[PipelineMetrics] = []
        
        # Prometheus Metrics
        self._init_prometheus_metrics()
        
        # System Monitoring
        self.system_metrics = {
            'cpu_percent': 0.0,
            'memory_percent': 0.0,
            'disk_usage_percent': 0.0,
            'network_io': {'bytes_sent': 0, 'bytes_recv': 0}
        }
        
        # Start background monitoring
        self._monitoring_task = None
        # Don't auto-start monitoring - let FastAPI handle it
    
    def _init_prometheus_metrics(self):
        """Initialize Prometheus metrics"""
        self.prometheus_metrics = {
            # Pipeline Metrics
            'pipelines_total': Counter(
                'etl_pipelines_total',
                'Total number of ETL pipelines executed',
                ['status'],
                registry=self.registry
            ),
            'pipeline_duration': Histogram(
                'etl_pipeline_duration_seconds',
                'Time spent executing ETL pipelines',
                registry=self.registry
            ),
            'records_processed': Counter(
                'etl_records_processed_total',
                'Total number of records processed',
                ['pipeline_type'],
                registry=self.registry
            ),
            'records_failed': Counter(
                'etl_records_failed_total',
                'Total number of records that failed processing',
                ['pipeline_type', 'error_type'],
                registry=self.registry
            ),
            
            # System Metrics
            'system_cpu_usage': Gauge(
                'system_cpu_usage_percent',
                'System CPU usage percentage',
                registry=self.registry
            ),
            'system_memory_usage': Gauge(
                'system_memory_usage_percent',
                'System memory usage percentage',
                registry=self.registry
            ),
            
            # Data Quality Metrics
            'data_quality_score': Histogram(
                'etl_data_quality_score',
                'Data quality score for processed datasets',
                registry=self.registry
            ),
        }
    
    def start_monitoring(self):
        """Start background system monitoring"""
        if not self._monitoring_task or self._monitoring_task.done():
            self._monitoring_task = asyncio.create_task(self._monitor_system())
    
    async def _monitor_system(self):
        """Background task to monitor system resources"""
        while True:
            try:
                # CPU and Memory
                cpu_percent = psutil.cpu_percent(interval=1)
                memory = psutil.virtual_memory()
                disk = psutil.disk_usage('/')
                network = psutil.net_io_counters()
                
                self.system_metrics.update({
                    'cpu_percent': float(cpu_percent),
                    'memory_percent': float(memory.percent),
                    'disk_usage_percent': float((disk.used / disk.total) * 100),
                    'network_io': {
                        'bytes_sent': int(network.bytes_sent),
                        'bytes_recv': int(network.bytes_recv)
                    },
                    'timestamp': datetime.now().isoformat()
                })
                
                # Update Prometheus metrics
                self.prometheus_metrics['system_cpu_usage'].set(cpu_percent)
                self.prometheus_metrics['system_memory_usage'].set(memory.percent)
                
                await asyncio.sleep(10)  # Update every 10 seconds
                
            except Exception as e:
                self.logger.error(f"Error monitoring system: {e}")
                await asyncio.sleep(30)  # Retry after 30 seconds on error
    
    def start_pipeline_monitoring(self, pipeline_id: str, pipeline_type: str = "batch") -> PipelineMetrics:
        """Start monitoring a new pipeline"""
        metrics = PipelineMetrics(
            pipeline_id=pipeline_id,
            start_time=datetime.now(),
        )
        
        self.active_pipelines[pipeline_id] = metrics
        self.logger.info(f"Started monitoring pipeline {pipeline_id}")
        
        return metrics
    
    def update_pipeline_metrics(self, pipeline_id: str, **kwargs):
        """Update metrics for an active pipeline"""
        if pipeline_id in self.active_pipelines:
            metrics = self.active_pipelines[pipeline_id]
            
            for key, value in kwargs.items():
                if hasattr(metrics, key):
                    setattr(metrics, key, value)
            
            self.logger.debug(f"Updated metrics for pipeline {pipeline_id}: {kwargs}")
    
    def finish_pipeline_monitoring(self, pipeline_id: str, status: str = "completed"):
        """Finish monitoring a pipeline and update final metrics"""
        if pipeline_id in self.active_pipelines:
            metrics = self.active_pipelines[pipeline_id]
            metrics.end_time = datetime.now()
            metrics.status = status
            
            # Calculate final metrics
            if metrics.records_processed > 0:
                metrics.throughput_records_per_second = (
                    metrics.records_processed / metrics.duration_seconds
                )
            
            # Update Prometheus metrics
            self.prometheus_metrics['pipelines_total'].labels(status=status).inc()
            self.prometheus_metrics['pipeline_duration'].observe(metrics.duration_seconds)
            
            if metrics.data_quality_score > 0:
                self.prometheus_metrics['data_quality_score'].observe(metrics.data_quality_score)
            
            # Move to history
            self.pipeline_history.append(metrics)
            del self.active_pipelines[pipeline_id]
            
            self.logger.info(f"Finished monitoring pipeline {pipeline_id} with status {status}")
            
            return metrics.to_dict()
    
    def get_pipeline_metrics(self, pipeline_id: str) -> Optional[Dict[str, Any]]:
        """Get current metrics for a pipeline"""
        if pipeline_id in self.active_pipelines:
            return self.active_pipelines[pipeline_id].to_dict()
        
        # Check history
        for metrics in self.pipeline_history:
            if metrics.pipeline_id == pipeline_id:
                return metrics.to_dict()
        
        return None
    
    def get_system_metrics(self) -> Dict[str, Any]:
        """Get current system metrics"""
        return self.system_metrics.copy()
    
    def get_pipeline_statistics(self, hours: int = 24) -> Dict[str, Any]:
        """Get pipeline statistics for the specified time period"""
        cutoff_time = datetime.now() - timedelta(hours=hours)
        
        recent_pipelines = [
            p for p in self.pipeline_history 
            if p.start_time >= cutoff_time
        ]
        
        if not recent_pipelines:
            return {
                'total_pipelines': 0,
                'successful_pipelines': 0,
                'failed_pipelines': 0,
                'average_duration_seconds': 0,
                'total_records_processed': 0,
                'average_throughput': 0,
                'average_data_quality_score': 0
            }
        
        successful = [p for p in recent_pipelines if p.status == 'completed']
        failed = [p for p in recent_pipelines if p.status == 'failed']
        
        total_records = sum(p.records_processed for p in recent_pipelines)
        total_duration = sum(p.duration_seconds for p in recent_pipelines)
        
        return {
            'total_pipelines': len(recent_pipelines),
            'successful_pipelines': len(successful),
            'failed_pipelines': len(failed),
            'success_rate': len(successful) / len(recent_pipelines) if recent_pipelines else 0,
            'average_duration_seconds': total_duration / len(recent_pipelines),
            'total_records_processed': total_records,
            'average_throughput': (total_records / total_duration) if total_duration > 0 else 0,
            'average_data_quality_score': sum(p.data_quality_score for p in recent_pipelines) / len(recent_pipelines),
            'time_period_hours': hours
        }
    
    def get_error_summary(self, hours: int = 24) -> List[Dict[str, Any]]:
        """Get error summary for the specified time period"""
        cutoff_time = datetime.now() - timedelta(hours=hours)
        
        failed_pipelines = [
            p for p in self.pipeline_history 
            if p.start_time >= cutoff_time and p.status == 'failed'
        ]
        
        errors = []
        for pipeline in failed_pipelines:
            for error in pipeline.error_messages:
                errors.append({
                    'pipeline_id': pipeline.pipeline_id,
                    'timestamp': pipeline.start_time.isoformat(),
                    'error_message': error,
                    'records_processed': pipeline.records_processed,
                    'duration_seconds': pipeline.duration_seconds
                })
        
        return errors
    
    async def close(self):
        """Cleanup monitoring resources"""
        if self._monitoring_task and not self._monitoring_task.done():
            self._monitoring_task.cancel()
            try:
                await self._monitoring_task
            except asyncio.CancelledError:
                pass
        
        self.logger.info("Metrics collector closed")


# Global metrics collector instance
metrics_collector = MetricsCollector()


def get_metrics_collector() -> MetricsCollector:
    """Get the global metrics collector instance"""
    return metrics_collector