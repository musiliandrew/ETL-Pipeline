"""
Advanced Configuration Management for ETL Portfolio
=================================================
Production-grade configuration with environment-specific settings
"""

from pydantic_settings import BaseSettings
from pydantic import Field
from typing import Optional, List
import os


class DatabaseConfig(BaseSettings):
    """Database configuration settings"""
    host: str = Field(default="localhost", env="DB_HOST")
    port: int = Field(default=5432, env="DB_PORT")
    name: str = Field(default="etl_portfolio", env="DB_NAME")
    user: str = Field(default="postgres", env="DB_USER")
    password: str = Field(default="password", env="DB_PASSWORD")
    
    # Connection Pool Settings
    pool_size: int = Field(default=20, env="DB_POOL_SIZE")
    max_overflow: int = Field(default=30, env="DB_MAX_OVERFLOW")
    pool_timeout: int = Field(default=30, env="DB_POOL_TIMEOUT")
    
    class Config:
        extra = "allow"
    
    @property
    def url(self) -> str:
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.name}"


class RedisConfig(BaseSettings):
    """Redis configuration for caching and session management"""
    host: str = Field(default="localhost", env="REDIS_HOST")
    port: int = Field(default=6379, env="REDIS_PORT")
    password: Optional[str] = Field(default=None, env="REDIS_PASSWORD")
    db: int = Field(default=0, env="REDIS_DB")
    
    @property
    def url(self) -> str:
        if self.password:
            return f"redis://:{self.password}@{self.host}:{self.port}/{self.db}"
        return f"redis://{self.host}:{self.port}/{self.db}"


class PipelineConfig(BaseSettings):
    """ETL Pipeline configuration"""
    # Processing Settings
    batch_size: int = Field(default=1000, env="PIPELINE_BATCH_SIZE")
    max_workers: int = Field(default=4, env="PIPELINE_MAX_WORKERS")
    retry_attempts: int = Field(default=3, env="PIPELINE_RETRY_ATTEMPTS")
    retry_delay: float = Field(default=1.0, env="PIPELINE_RETRY_DELAY")
    
    # Data Quality Settings
    data_quality_threshold: float = Field(default=0.95, env="DATA_QUALITY_THRESHOLD")
    null_percentage_threshold: float = Field(default=0.1, env="NULL_PERCENTAGE_THRESHOLD")
    duplicate_threshold: float = Field(default=0.05, env="DUPLICATE_THRESHOLD")
    
    # Performance Settings
    enable_performance_monitoring: bool = Field(default=True, env="ENABLE_PERFORMANCE_MONITORING")
    metrics_collection_interval: int = Field(default=10, env="METRICS_COLLECTION_INTERVAL")


class StreamingConfig(BaseSettings):
    """Streaming pipeline configuration"""
    enable_streaming: bool = Field(default=True, env="ENABLE_STREAMING")
    stream_buffer_size: int = Field(default=100, env="STREAM_BUFFER_SIZE")
    stream_batch_timeout: float = Field(default=5.0, env="STREAM_BATCH_TIMEOUT")
    
    # Kafka/Message Broker (if needed)
    broker_url: Optional[str] = Field(default=None, env="KAFKA_BROKER_URL")
    topic_name: str = Field(default="etl_data_stream", env="KAFKA_TOPIC")


class MonitoringConfig(BaseSettings):
    """Monitoring and observability settings"""
    enable_prometheus: bool = Field(default=True, env="ENABLE_PROMETHEUS")
    prometheus_port: int = Field(default=8001, env="PROMETHEUS_PORT")
    
    # Logging
    log_level: str = Field(default="INFO", env="LOG_LEVEL")
    enable_structured_logging: bool = Field(default=True, env="ENABLE_STRUCTURED_LOGGING")
    
    # Alerting
    alert_webhook_url: Optional[str] = Field(default=None, env="ALERT_WEBHOOK_URL")
    error_threshold: int = Field(default=10, env="ERROR_THRESHOLD")


class AppConfig(BaseSettings):
    """Main application configuration"""
    # Application Settings
    app_name: str = Field(default="ETL Portfolio Showcase", env="APP_NAME")
    version: str = Field(default="1.0.0", env="APP_VERSION")
    debug: bool = Field(default=False, env="DEBUG")
    
    # API Settings
    api_host: str = Field(default="0.0.0.0", env="API_HOST")
    api_port: int = Field(default=8000, env="API_PORT")
    
    # CORS Settings
    cors_origins: List[str] = Field(
        default=["http://localhost:3000", "http://localhost:5173", "http://localhost:5174"],
        env="CORS_ORIGINS"
    )
    
    # File Paths
    data_input_path: str = Field(default="data/input", env="DATA_INPUT_PATH")
    data_processed_path: str = Field(default="data/processed", env="DATA_PROCESSED_PATH")
    data_deadletter_path: str = Field(default="data/deadletter", env="DATA_DEADLETTER_PATH")
    schema_registry_path: str = Field(default="schemas", env="SCHEMA_REGISTRY_PATH")
    
    # Security
    secret_key: str = Field(default="dev-secret-key-change-in-production", env="SECRET_KEY")
    
    # Component Configurations
    database: DatabaseConfig = DatabaseConfig()
    redis: RedisConfig = RedisConfig()
    pipeline: PipelineConfig = PipelineConfig()
    streaming: StreamingConfig = StreamingConfig()
    monitoring: MonitoringConfig = MonitoringConfig()
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False
        extra = "allow"


# Global configuration instance
config = None

def get_config() -> AppConfig:
    """Get application configuration"""
    global config
    if config is None:
        config = AppConfig()
    return config