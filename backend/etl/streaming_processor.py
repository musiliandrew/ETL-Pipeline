"""
Streaming Data Processor
========================
Real-time data processing with micro-batching and performance comparison
"""

import asyncio
import pandas as pd
from typing import Dict, List, Any, Optional, AsyncGenerator, Callable
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
import json
import time
from enum import Enum
import structlog
from pathlib import Path
import queue
import threading
from concurrent.futures import ThreadPoolExecutor


class ProcessingMode(Enum):
    BATCH = "batch"
    STREAMING = "streaming"
    MICRO_BATCH = "micro_batch"


@dataclass
class StreamRecord:
    """Individual stream record"""
    id: str
    data: Dict[str, Any]
    timestamp: datetime
    source: str = "unknown"
    
    def to_dict(self) -> Dict[str, Any]:
        data = asdict(self)
        data['timestamp'] = self.timestamp.isoformat()
        return data


@dataclass
class StreamBatch:
    """Batch of stream records"""
    batch_id: str
    records: List[StreamRecord]
    created_at: datetime
    processing_mode: ProcessingMode
    
    def to_dataframe(self) -> pd.DataFrame:
        """Convert batch to pandas DataFrame"""
        if not self.records:
            return pd.DataFrame()
        
        data = [record.data for record in self.records]
        df = pd.DataFrame(data)
        
        # Add metadata columns
        df['_stream_id'] = [r.id for r in self.records]
        df['_stream_timestamp'] = [r.timestamp for r in self.records]
        df['_stream_source'] = [r.source for r in self.records]
        
        return df
    
    def to_dict(self) -> Dict[str, Any]:
        data = asdict(self)
        data['created_at'] = self.created_at.isoformat()
        data['processing_mode'] = self.processing_mode.value
        data['records'] = [r.to_dict() for r in self.records]
        return data


class StreamingDataGenerator:
    """Generate streaming data for demonstration"""
    
    def __init__(self):
        self.logger = structlog.get_logger()
        self.is_running = False
        self.data_patterns = {
            'clean': self._generate_clean_record,
            'messy': self._generate_messy_record,
            'schema_evolution': self._generate_evolving_record,
            'chaos': self._generate_chaotic_record
        }
    
    async def generate_stream(
        self, 
        pattern: str = 'clean',
        records_per_second: int = 10,
        duration_seconds: Optional[int] = None
    ) -> AsyncGenerator[StreamRecord, None]:
        """Generate continuous stream of records"""
        
        self.is_running = True
        start_time = datetime.now()
        record_count = 0
        
        generator = self.data_patterns.get(pattern, self._generate_clean_record)
        
        while self.is_running:
            if duration_seconds and (datetime.now() - start_time).seconds >= duration_seconds:
                break
            
            # Generate record
            record_data = generator()
            record = StreamRecord(
                id=f"stream_{record_count:06d}",
                data=record_data,
                timestamp=datetime.now(),
                source=f"generator_{pattern}"
            )
            
            yield record
            record_count += 1
            
            # Rate limiting
            await asyncio.sleep(1.0 / records_per_second)
        
        self.logger.info(f"Generated {record_count} records in stream mode")
    
    def _generate_clean_record(self) -> Dict[str, Any]:
        """Generate clean, well-structured record"""
        import random
        from faker import Faker
        fake = Faker()
        
        return {
            'user_id': fake.uuid4(),
            'age': random.randint(18, 80),
            'sign_up_date': fake.date_between(start_date='-2y', end_date='today').isoformat(),
            'is_active': random.choice([True, False]),
            'email': fake.email(),
            'city': fake.city(),
            'country': fake.country()
        }
    
    def _generate_messy_record(self) -> Dict[str, Any]:
        """Generate messy data with quality issues"""
        import random
        
        clean_record = self._generate_clean_record()
        
        # Introduce data quality issues
        if random.random() < 0.1:  # 10% null values
            clean_record['age'] = None
        
        if random.random() < 0.05:  # 5% invalid emails
            clean_record['email'] = "invalid-email"
        
        if random.random() < 0.08:  # 8% future dates
            clean_record['sign_up_date'] = "2030-01-01"
        
        if random.random() < 0.03:  # 3% negative ages
            clean_record['age'] = -5
        
        return clean_record
    
    def _generate_evolving_record(self) -> Dict[str, Any]:
        """Generate records that evolve schema over time"""
        import random
        
        base_record = self._generate_clean_record()
        
        # Gradually introduce new fields
        evolution_stage = (datetime.now().second // 10) % 4
        
        if evolution_stage >= 1:
            base_record['subscription_tier'] = random.choice(['basic', 'premium', 'enterprise'])
        
        if evolution_stage >= 2:
            base_record['last_login'] = datetime.now().isoformat()
            base_record['login_count'] = random.randint(0, 1000)
        
        if evolution_stage >= 3:
            base_record['preferences'] = {
                'notifications': random.choice([True, False]),
                'theme': random.choice(['light', 'dark']),
                'language': random.choice(['en', 'es', 'fr', 'de'])
            }
        
        return base_record
    
    def _generate_chaotic_record(self) -> Dict[str, Any]:
        """Generate highly chaotic data for stress testing"""
        import random
        
        # Start with messy record
        record = self._generate_messy_record()
        
        # Add chaos
        chaos_events = [
            lambda: record.update({'duplicate_field': record.get('user_id')}),
            lambda: record.update({'malformed_json': '{"incomplete": json'}),
            lambda: record.update({'extreme_age': 999}),
            lambda: record.update({'empty_string': ''}),
            lambda: record.update({'unicode_chaos': '🚀💥🔥'}),
            lambda: record.pop('user_id', None),  # Remove required field
        ]
        
        # Apply random chaos events
        for event in random.sample(chaos_events, random.randint(0, 3)):
            try:
                event()
            except:
                pass
        
        return record
    
    def stop_generation(self):
        """Stop the stream generation"""
        self.is_running = False


class StreamProcessor:
    """Process streaming data with different strategies"""
    
    def __init__(self, batch_size: int = 100, batch_timeout_seconds: float = 5.0):
        self.batch_size = batch_size
        self.batch_timeout_seconds = batch_timeout_seconds
        self.logger = structlog.get_logger()
        
        # Processing queues
        self.record_queue: queue.Queue = queue.Queue()
        self.batch_queue: queue.Queue = queue.Queue()
        
        # Processing threads
        self.batch_processor_thread: Optional[threading.Thread] = None
        self.is_processing = False
        
        # Statistics
        self.stats = {
            'records_processed': 0,
            'batches_created': 0,
            'processing_errors': 0,
            'start_time': None,
            'last_batch_time': None
        }
    
    def start_processing(self):
        """Start the stream processing"""
        if self.is_processing:
            return
        
        self.is_processing = True
        self.stats['start_time'] = datetime.now()
        
        # Start batch creation thread
        self.batch_processor_thread = threading.Thread(
            target=self._batch_creation_worker,
            daemon=True
        )
        self.batch_processor_thread.start()
        
        self.logger.info("Stream processor started")
    
    def stop_processing(self):
        """Stop the stream processing"""
        self.is_processing = False
        
        if self.batch_processor_thread and self.batch_processor_thread.is_alive():
            self.batch_processor_thread.join(timeout=5)
        
        self.logger.info("Stream processor stopped")
    
    def add_record(self, record: StreamRecord):
        """Add a record to the processing queue"""
        if self.is_processing:
            self.record_queue.put(record)
            self.stats['records_processed'] += 1
    
    def _batch_creation_worker(self):
        """Worker thread that creates batches from individual records"""
        current_batch_records = []
        last_batch_time = time.time()
        
        while self.is_processing:
            try:
                # Try to get a record with timeout
                try:
                    record = self.record_queue.get(timeout=1.0)
                    current_batch_records.append(record)
                except queue.Empty:
                    pass
                
                current_time = time.time()
                
                # Create batch if we have enough records or timeout
                should_create_batch = (
                    len(current_batch_records) >= self.batch_size or
                    (current_batch_records and 
                     (current_time - last_batch_time) >= self.batch_timeout_seconds)
                )
                
                if should_create_batch:
                    batch = StreamBatch(
                        batch_id=f"batch_{self.stats['batches_created']:06d}",
                        records=current_batch_records.copy(),
                        created_at=datetime.now(),
                        processing_mode=ProcessingMode.MICRO_BATCH
                    )
                    
                    self.batch_queue.put(batch)
                    self.stats['batches_created'] += 1
                    self.stats['last_batch_time'] = datetime.now()
                    
                    current_batch_records.clear()
                    last_batch_time = current_time
                    
                    self.logger.debug(f"Created batch with {len(batch.records)} records")
            
            except Exception as e:
                self.logger.error(f"Error in batch creation worker: {e}")
                self.stats['processing_errors'] += 1
        
        # Create final batch if we have remaining records
        if current_batch_records:
            final_batch = StreamBatch(
                batch_id=f"batch_final_{self.stats['batches_created']:06d}",
                records=current_batch_records,
                created_at=datetime.now(),
                processing_mode=ProcessingMode.MICRO_BATCH
            )
            self.batch_queue.put(final_batch)
    
    def get_next_batch(self, timeout: float = 1.0) -> Optional[StreamBatch]:
        """Get the next batch for processing"""
        try:
            return self.batch_queue.get(timeout=timeout)
        except queue.Empty:
            return None
    
    def get_processing_stats(self) -> Dict[str, Any]:
        """Get current processing statistics"""
        stats = self.stats.copy()
        
        if stats['start_time']:
            duration = (datetime.now() - stats['start_time']).total_seconds()
            stats['duration_seconds'] = duration
            stats['records_per_second'] = stats['records_processed'] / duration if duration > 0 else 0
            stats['batches_per_minute'] = (stats['batches_created'] / duration) * 60 if duration > 0 else 0
        
        stats['queue_sizes'] = {
            'record_queue': self.record_queue.qsize(),
            'batch_queue': self.batch_queue.qsize()
        }
        
        return stats


class StreamingBenchmark:
    """Benchmark streaming vs batch processing performance"""
    
    def __init__(self):
        self.logger = structlog.get_logger()
        self.results = []
    
    async def run_performance_comparison(
        self,
        data_pattern: str = 'clean',
        record_count: int = 1000,
        batch_sizes: List[int] = None
    ) -> Dict[str, Any]:
        """Run performance comparison between different processing modes"""
        
        if batch_sizes is None:
            batch_sizes = [10, 50, 100, 500]
        
        results = {
            'test_config': {
                'data_pattern': data_pattern,
                'record_count': record_count,
                'batch_sizes_tested': batch_sizes
            },
            'batch_processing': {},
            'streaming_processing': {},
            'comparison': {}
        }
        
        # Test batch processing
        self.logger.info("Testing batch processing...")
        batch_result = await self._test_batch_processing(data_pattern, record_count)
        results['batch_processing'] = batch_result
        
        # Test streaming processing with different batch sizes
        self.logger.info("Testing streaming processing...")
        streaming_results = {}
        
        for batch_size in batch_sizes:
            stream_result = await self._test_streaming_processing(
                data_pattern, record_count, batch_size
            )
            streaming_results[f'batch_size_{batch_size}'] = stream_result
        
        results['streaming_processing'] = streaming_results
        
        # Generate comparison
        results['comparison'] = self._generate_comparison(batch_result, streaming_results)
        
        return results
    
    async def _test_batch_processing(self, pattern: str, record_count: int) -> Dict[str, Any]:
        """Test traditional batch processing"""
        generator = StreamingDataGenerator()
        
        # Generate all records at once
        start_time = time.time()
        records = []
        
        async for record in generator.generate_stream(pattern, 1000, duration_seconds=1):
            records.append(record)
            if len(records) >= record_count:
                break
        
        generation_time = time.time() - start_time
        
        # Process as single batch
        start_time = time.time()
        batch = StreamBatch(
            batch_id="benchmark_batch",
            records=records,
            created_at=datetime.now(),
            processing_mode=ProcessingMode.BATCH
        )
        
        df = batch.to_dataframe()
        processing_time = time.time() - start_time
        
        return {
            'mode': 'batch',
            'records_processed': len(records),
            'generation_time_seconds': generation_time,
            'processing_time_seconds': processing_time,
            'total_time_seconds': generation_time + processing_time,
            'throughput_records_per_second': len(records) / (generation_time + processing_time),
            'memory_usage_mb': df.memory_usage(deep=True).sum() / 1024 / 1024
        }
    
    async def _test_streaming_processing(
        self, pattern: str, record_count: int, batch_size: int
    ) -> Dict[str, Any]:
        """Test streaming processing with micro-batches"""
        
        generator = StreamingDataGenerator()
        processor = StreamProcessor(batch_size=batch_size, batch_timeout_seconds=1.0)
        
        processor.start_processing()
        
        start_time = time.time()
        records_generated = 0
        batches_processed = 0
        
        # Generate and process records concurrently
        async def generate_records():
            nonlocal records_generated
            async for record in generator.generate_stream(pattern, 500):  # 500 rps
                processor.add_record(record)
                records_generated += 1
                if records_generated >= record_count:
                    break
        
        def process_batches():
            nonlocal batches_processed
            while True:
                batch = processor.get_next_batch(timeout=0.1)
                if batch is None:
                    if records_generated >= record_count and processor.record_queue.empty():
                        break
                    continue
                
                # Simulate processing
                df = batch.to_dataframe()
                batches_processed += 1
        
        # Run generation and processing concurrently
        await asyncio.gather(
            generate_records(),
            asyncio.get_event_loop().run_in_executor(None, process_batches)
        )
        
        total_time = time.time() - start_time
        processor.stop_processing()
        
        stats = processor.get_processing_stats()
        
        return {
            'mode': 'streaming',
            'batch_size': batch_size,
            'records_processed': records_generated,
            'batches_created': batches_processed,
            'total_time_seconds': total_time,
            'throughput_records_per_second': records_generated / total_time,
            'average_batch_size': records_generated / batches_processed if batches_processed > 0 else 0,
            'processing_latency_seconds': stats.get('duration_seconds', 0),
            'queue_efficiency': {
                'max_record_queue_size': stats['queue_sizes']['record_queue'],
                'max_batch_queue_size': stats['queue_sizes']['batch_queue']
            }
        }
    
    def _generate_comparison(
        self, 
        batch_result: Dict[str, Any], 
        streaming_results: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Generate performance comparison analysis"""
        
        best_streaming = min(
            streaming_results.values(),
            key=lambda x: x['total_time_seconds']
        )
        
        return {
            'winner': {
                'fastest_mode': 'batch' if batch_result['total_time_seconds'] < best_streaming['total_time_seconds'] else 'streaming',
                'batch_time': batch_result['total_time_seconds'],
                'best_streaming_time': best_streaming['total_time_seconds'],
                'time_difference_seconds': abs(batch_result['total_time_seconds'] - best_streaming['total_time_seconds'])
            },
            'throughput_comparison': {
                'batch_throughput': batch_result['throughput_records_per_second'],
                'best_streaming_throughput': best_streaming['throughput_records_per_second'],
                'throughput_improvement': (best_streaming['throughput_records_per_second'] / batch_result['throughput_records_per_second'] - 1) * 100
            },
            'recommendations': self._generate_recommendations(batch_result, streaming_results)
        }
    
    def _generate_recommendations(
        self, 
        batch_result: Dict[str, Any], 
        streaming_results: Dict[str, Any]
    ) -> List[str]:
        """Generate recommendations based on benchmark results"""
        
        recommendations = []
        
        # Throughput analysis
        batch_throughput = batch_result['throughput_records_per_second']
        streaming_throughputs = [r['throughput_records_per_second'] for r in streaming_results.values()]
        max_streaming_throughput = max(streaming_throughputs)
        
        if max_streaming_throughput > batch_throughput * 1.1:
            recommendations.append("Streaming processing shows significantly better throughput")
        elif batch_throughput > max_streaming_throughput * 1.1:
            recommendations.append("Batch processing shows better overall throughput")
        else:
            recommendations.append("Batch and streaming performance are comparable")
        
        # Latency analysis
        recommendations.append("Use streaming for real-time requirements with sub-second latency needs")
        recommendations.append("Use batch processing for large-scale data processing with high throughput requirements")
        
        # Resource efficiency
        if len(streaming_results) > 1:
            best_batch_size = min(streaming_results.keys(), 
                                key=lambda k: streaming_results[k]['total_time_seconds'])
            recommendations.append(f"Optimal streaming batch size appears to be {best_batch_size.split('_')[-1]} records")
        
        return recommendations


# Global instances
streaming_generator = StreamingDataGenerator()
stream_processor = StreamProcessor()
streaming_benchmark = StreamingBenchmark()


def get_streaming_generator() -> StreamingDataGenerator:
    """Get the global streaming generator instance"""
    return streaming_generator


def get_stream_processor() -> StreamProcessor:
    """Get the global stream processor instance"""
    return stream_processor


def get_streaming_benchmark() -> StreamingBenchmark:
    """Get the global streaming benchmark instance"""
    return streaming_benchmark