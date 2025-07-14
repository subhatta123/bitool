"""
Performance utilities for ConvaBI Application
Provides caching, connection pooling, and performance monitoring
"""

import time
import logging
from typing import Dict, Any, Optional, Callable
from functools import wraps
from django.core.cache import cache
from django.conf import settings
import hashlib
import json

logger = logging.getLogger(__name__)

class PerformanceCache:
    """Enhanced caching utilities"""
    
    @staticmethod
    def cache_with_timeout(key_prefix: str, timeout: int = 300):
        """Decorator to cache function results"""
        def decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs):
                # Generate cache key
                cache_key = PerformanceCache._generate_cache_key(key_prefix, args, kwargs)
                
                # Try to get from cache
                result = cache.get(cache_key)
                if result is not None:
                    logger.debug(f"Cache hit for {cache_key}")
                    return result
                
                # Execute function and cache result
                result = func(*args, **kwargs)
                cache.set(cache_key, result, timeout=timeout)
                logger.debug(f"Cached result for {cache_key}")
                return result
            
            return wrapper
        return decorator
    
    @staticmethod
    def _generate_cache_key(prefix: str, args: tuple, kwargs: dict) -> str:
        """Generate cache key from function arguments"""
        key_data = f"{prefix}_{args}_{sorted(kwargs.items())}"
        return f"perf_cache_{hashlib.md5(key_data.encode()).hexdigest()}"
    
    @staticmethod
    def invalidate_cache_pattern(pattern: str):
        """Invalidate cache keys matching pattern"""
        try:
            # This is a simplified implementation
            # In production, use a cache backend that supports pattern deletion
            cache.clear()
            logger.info(f"Cache cleared for pattern: {pattern}")
        except Exception as e:
            logger.error(f"Failed to invalidate cache: {e}")

class QueryPerformanceMonitor:
    """Monitor and log query performance"""
    
    @staticmethod
    def monitor_query_performance(query_type: str = "unknown"):
        """Decorator to monitor query performance"""
        def decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs):
                start_time = time.time()
                
                try:
                    result = func(*args, **kwargs)
                    execution_time = time.time() - start_time
                    
                    # Log performance metrics
                    QueryPerformanceMonitor._log_performance(
                        query_type, func.__name__, execution_time, success=True
                    )
                    
                    return result
                    
                except Exception as e:
                    execution_time = time.time() - start_time
                    QueryPerformanceMonitor._log_performance(
                        query_type, func.__name__, execution_time, success=False, error=str(e)
                    )
                    raise
            
            return wrapper
        return decorator
    
    @staticmethod
    def _log_performance(query_type: str, function_name: str, execution_time: float, 
                        success: bool, error: Optional[str] = None):
        """Log performance metrics"""
        try:
            log_data = {
                'query_type': query_type,
                'function': function_name,
                'execution_time': round(execution_time, 3),
                'success': success,
                'timestamp': time.time()
            }
            
            if error:
                log_data['error'] = error
            
            # Log based on performance thresholds
            if execution_time > 10:  # Slow query threshold
                logger.warning(f"SLOW_QUERY: {json.dumps(log_data)}")
            elif execution_time > 5:
                logger.info(f"MEDIUM_QUERY: {json.dumps(log_data)}")
            else:
                logger.debug(f"FAST_QUERY: {json.dumps(log_data)}")
            
        except Exception as e:
            logger.error(f"Failed to log performance: {e}")

class ConnectionPoolManager:
    """Manage database connection pools"""
    
    def __init__(self):
        self.pools = {}
        self.pool_stats = {}
    
    def get_pool_stats(self, pool_name: str) -> Dict[str, Any]:
        """Get connection pool statistics"""
        return self.pool_stats.get(pool_name, {
            'active_connections': 0,
            'idle_connections': 0,
            'total_requests': 0,
            'failed_requests': 0
        })
    
    def record_connection_usage(self, pool_name: str, success: bool):
        """Record connection usage statistics"""
        if pool_name not in self.pool_stats:
            self.pool_stats[pool_name] = {
                'active_connections': 0,
                'idle_connections': 0,
                'total_requests': 0,
                'failed_requests': 0
            }
        
        self.pool_stats[pool_name]['total_requests'] += 1
        if not success:
            self.pool_stats[pool_name]['failed_requests'] += 1

class MemoryManager:
    """Memory management utilities for large datasets"""
    
    @staticmethod
    def chunk_processor(chunk_size: int = 1000):
        """Decorator to process large datasets in chunks"""
        def decorator(func):
            @wraps(func)
            def wrapper(data, *args, **kwargs):
                import pandas as pd
                
                if isinstance(data, pd.DataFrame) and len(data) > chunk_size:
                    # Process in chunks
                    results = []
                    for i in range(0, len(data), chunk_size):
                        chunk = data.iloc[i:i + chunk_size]
                        chunk_result = func(chunk, *args, **kwargs)
                        results.append(chunk_result)
                    
                    # Combine results if they're DataFrames
                    if results and isinstance(results[0], pd.DataFrame):
                        return pd.concat(results, ignore_index=True)
                    else:
                        return results
                else:
                    return func(data, *args, **kwargs)
            
            return wrapper
        return decorator
    
    @staticmethod
    def get_memory_usage_mb() -> float:
        """Get current memory usage in MB"""
        try:
            import psutil
            import os
            process = psutil.Process(os.getpid())
            memory_mb = process.memory_info().rss / 1024 / 1024
            return round(memory_mb, 2)
        except ImportError:
            logger.warning("psutil not available for memory monitoring")
            return 0.0
        except Exception as e:
            logger.error(f"Failed to get memory usage: {e}")
            return 0.0

class AsyncHelper:
    """Utilities for async processing"""
    
    @staticmethod
    def run_in_background(task_name: str = "background_task"):
        """Decorator to run function in background using Celery"""
        def decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs):
                try:
                    from celery import current_app
                    
                    # Create Celery task
                    task = current_app.send_task(
                        f'utils.performance.{func.__name__}',
                        args=args,
                        kwargs=kwargs
                    )
                    
                    logger.info(f"Background task {task_name} started: {task.id}")
                    return {'task_id': task.id, 'status': 'started'}
                    
                except Exception as e:
                    logger.error(f"Failed to start background task {task_name}: {e}")
                    # Fallback to synchronous execution
                    return func(*args, **kwargs)
            
            return wrapper
        return decorator

# Performance monitoring decorators
def log_execution_time(func):
    """Simple execution time logging decorator"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        execution_time = time.time() - start_time
        
        logger.info(f"{func.__name__} executed in {execution_time:.3f} seconds")
        return result
    
    return wrapper

def profile_memory(func):
    """Memory profiling decorator"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        initial_memory = MemoryManager.get_memory_usage_mb()
        result = func(*args, **kwargs)
        final_memory = MemoryManager.get_memory_usage_mb()
        
        memory_delta = final_memory - initial_memory
        logger.info(f"{func.__name__} memory usage: {memory_delta:.2f} MB delta")
        
        return result
    
    return wrapper

# Global performance manager instances
performance_cache = PerformanceCache()
query_monitor = QueryPerformanceMonitor()
connection_pool_manager = ConnectionPoolManager()
memory_manager = MemoryManager()
async_helper = AsyncHelper() 