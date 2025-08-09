"""
SQL Query Interface for Data Scientists
======================================
Interactive SQL console with query optimization, caching, and business intelligence features
"""

import pandas as pd
import sqlalchemy as sa
from sqlalchemy import create_engine, text, inspect
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
import time
import json
import re
import hashlib
import structlog
from enum import Enum


class QueryType(Enum):
    SELECT = "select"
    INSERT = "insert"
    UPDATE = "update"
    DELETE = "delete"
    DDL = "ddl"
    UNKNOWN = "unknown"


@dataclass
class QueryResult:
    """SQL query execution result"""
    query_id: str
    sql_query: str
    result_data: Optional[pd.DataFrame]
    execution_time_seconds: float
    row_count: int
    column_count: int
    query_type: QueryType
    timestamp: datetime
    cached: bool = False
    error_message: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        data = asdict(self)
        data['timestamp'] = self.timestamp.isoformat()
        data['query_type'] = self.query_type.value
        
        # Convert DataFrame to dict if present
        if self.result_data is not None:
            data['result_data'] = {
                'columns': self.result_data.columns.tolist(),
                'data': self.result_data.to_dict('records'),
                'dtypes': {col: str(dtype) for col, dtype in self.result_data.dtypes.items()}
            }
        
        return data


@dataclass
class QueryTemplate:
    """Pre-built query templates for common business questions"""
    name: str
    description: str
    sql_template: str
    parameters: List[Dict[str, Any]]
    category: str
    example_usage: str
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class QueryCache:
    """Cache frequently used queries for better performance"""
    
    def __init__(self, max_size: int = 100, ttl_seconds: int = 3600):
        self.cache: Dict[str, Tuple[QueryResult, datetime]] = {}
        self.max_size = max_size
        self.ttl_seconds = ttl_seconds
        self.logger = structlog.get_logger()
    
    def _generate_cache_key(self, sql_query: str) -> str:
        """Generate cache key from SQL query"""
        # Normalize query for consistent caching
        normalized = re.sub(r'\s+', ' ', sql_query.strip().lower())
        return hashlib.md5(normalized.encode()).hexdigest()
    
    def get(self, sql_query: str) -> Optional[QueryResult]:
        """Get cached query result"""
        cache_key = self._generate_cache_key(sql_query)
        
        if cache_key in self.cache:
            result, timestamp = self.cache[cache_key]
            
            # Check if cache entry is still valid
            if datetime.now() - timestamp < timedelta(seconds=self.ttl_seconds):
                result.cached = True
                return result
            else:
                # Remove expired entry
                del self.cache[cache_key]
        
        return None
    
    def put(self, sql_query: str, result: QueryResult):
        """Cache query result"""
        cache_key = self._generate_cache_key(sql_query)
        
        # Implement LRU eviction if cache is full
        if len(self.cache) >= self.max_size:
            # Remove oldest entry
            oldest_key = min(self.cache.keys(), key=lambda k: self.cache[k][1])
            del self.cache[oldest_key]
        
        self.cache[cache_key] = (result, datetime.now())
        self.logger.debug(f"Cached query result for key {cache_key[:8]}")
    
    def clear(self):
        """Clear all cached results"""
        self.cache.clear()
        self.logger.info("Query cache cleared")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        now = datetime.now()
        valid_entries = sum(
            1 for _, timestamp in self.cache.values()
            if now - timestamp < timedelta(seconds=self.ttl_seconds)
        )
        
        return {
            'total_entries': len(self.cache),
            'valid_entries': valid_entries,
            'expired_entries': len(self.cache) - valid_entries,
            'cache_hit_ratio': valid_entries / len(self.cache) if self.cache else 0,
            'max_size': self.max_size,
            'ttl_seconds': self.ttl_seconds
        }


class QueryAnalyzer:
    """Analyze SQL queries for optimization and security"""
    
    def __init__(self):
        self.logger = structlog.get_logger()
        
        # Dangerous patterns to detect
        self.dangerous_patterns = [
            (r'DROP\s+TABLE', 'DROP TABLE detected'),
            (r'DELETE\s+FROM\s+\w+\s*;?\s*$', 'DELETE without WHERE clause'),
            (r'UPDATE\s+\w+\s+SET.*;\s*$', 'UPDATE without WHERE clause'),
            (r'TRUNCATE', 'TRUNCATE operation'),
            (r'ALTER\s+TABLE', 'Schema modification'),
            (r'CREATE\s+TABLE', 'Table creation'),
        ]
        
        # Performance anti-patterns
        self.performance_issues = [
            (r'SELECT\s+\*\s+FROM', 'SELECT * can be inefficient'),
            (r'LIKE\s+\'%.*%\'', 'Leading wildcard LIKE can be slow'),
            (r'OR.*OR.*OR', 'Multiple OR conditions may need optimization'),
            (r'NOT\s+IN\s*\(', 'NOT IN can be slow with NULLs'),
        ]
    
    def analyze_query(self, sql_query: str) -> Dict[str, Any]:
        """Analyze SQL query for issues and suggestions"""
        analysis = {
            'query_type': self._detect_query_type(sql_query),
            'security_issues': [],
            'performance_warnings': [],
            'suggestions': [],
            'estimated_complexity': self._estimate_complexity(sql_query),
            'query_length': len(sql_query),
            'table_count': self._count_tables(sql_query)
        }
        
        # Check for security issues
        for pattern, message in self.dangerous_patterns:
            if re.search(pattern, sql_query, re.IGNORECASE):
                analysis['security_issues'].append({
                    'issue': message,
                    'severity': 'high',
                    'pattern': pattern
                })
        
        # Check for performance issues
        for pattern, message in self.performance_issues:
            if re.search(pattern, sql_query, re.IGNORECASE):
                analysis['performance_warnings'].append({
                    'warning': message,
                    'severity': 'medium',
                    'pattern': pattern
                })
        
        # Generate suggestions
        analysis['suggestions'] = self._generate_suggestions(sql_query, analysis)
        
        return analysis
    
    def _detect_query_type(self, sql_query: str) -> QueryType:
        """Detect the type of SQL query"""
        query_upper = sql_query.strip().upper()
        
        if query_upper.startswith('SELECT'):
            return QueryType.SELECT
        elif query_upper.startswith('INSERT'):
            return QueryType.INSERT
        elif query_upper.startswith('UPDATE'):
            return QueryType.UPDATE
        elif query_upper.startswith('DELETE'):
            return QueryType.DELETE
        elif any(query_upper.startswith(cmd) for cmd in ['CREATE', 'ALTER', 'DROP', 'TRUNCATE']):
            return QueryType.DDL
        else:
            return QueryType.UNKNOWN
    
    def _estimate_complexity(self, sql_query: str) -> str:
        """Estimate query complexity based on keywords"""
        complexity_score = 0
        
        # Count complexity indicators
        indicators = {
            r'\bJOIN\b': 2,
            r'\bSUBQUERY\b|\bSELECT.*FROM.*\(.*SELECT': 3,
            r'\bGROUP BY\b': 1,
            r'\bORDER BY\b': 1,
            r'\bHAVING\b': 2,
            r'\bWINDOW\b': 3,
            r'\bCTE\b|\bWITH\b': 2,
            r'\bUNION\b': 2
        }
        
        for pattern, score in indicators.items():
            if re.search(pattern, sql_query, re.IGNORECASE):
                complexity_score += score
        
        if complexity_score <= 2:
            return 'simple'
        elif complexity_score <= 5:
            return 'moderate'
        else:
            return 'complex'
    
    def _count_tables(self, sql_query: str) -> int:
        """Count the number of tables referenced in the query"""
        # Simple heuristic - count FROM and JOIN clauses
        from_count = len(re.findall(r'\bFROM\s+(\w+)', sql_query, re.IGNORECASE))
        join_count = len(re.findall(r'\bJOIN\s+(\w+)', sql_query, re.IGNORECASE))
        return from_count + join_count
    
    def _generate_suggestions(self, sql_query: str, analysis: Dict[str, Any]) -> List[str]:
        """Generate optimization suggestions"""
        suggestions = []
        
        # Suggestions based on analysis
        if analysis['query_type'] == QueryType.SELECT:
            if 'SELECT *' in sql_query.upper():
                suggestions.append("Consider specifying only needed columns instead of SELECT *")
            
            if analysis['table_count'] > 3:
                suggestions.append("Consider if all JOINs are necessary and properly indexed")
            
            if 'ORDER BY' in sql_query.upper() and 'LIMIT' not in sql_query.upper():
                suggestions.append("Consider adding LIMIT clause when using ORDER BY")
        
        if analysis['estimated_complexity'] == 'complex':
            suggestions.append("Consider breaking complex query into smaller parts or using views")
        
        if len(analysis['performance_warnings']) > 0:
            suggestions.append("Review performance warnings to optimize query execution")
        
        return suggestions


class SQLInterface:
    """Main SQL interface for data scientists"""
    
    def __init__(self, database_url: str):
        self.engine = create_engine(database_url)
        self.cache = QueryCache()
        self.analyzer = QueryAnalyzer()
        self.logger = structlog.get_logger()
        
        # Query execution history
        self.query_history: List[QueryResult] = []
        
        # Load pre-built templates
        self.templates = self._load_query_templates()
    
    def _load_query_templates(self) -> List[QueryTemplate]:
        """Load pre-built query templates for common business questions"""
        return [
            QueryTemplate(
                name="active_users_over_age",
                description="Find active users over a specific age",
                sql_template="""
                SELECT user_id, age, sign_up_date, is_active 
                FROM users 
                WHERE is_active = true 
                  AND age > {min_age}
                  AND sign_up_date IS NOT NULL
                ORDER BY age DESC
                LIMIT {limit}
                """,
                parameters=[
                    {"name": "min_age", "type": "integer", "default": 30, "description": "Minimum age filter"},
                    {"name": "limit", "type": "integer", "default": 100, "description": "Maximum rows to return"}
                ],
                category="user_analysis",
                example_usage="Find active users over 30 years old"
            ),
            QueryTemplate(
                name="recent_signups",
                description="Users who signed up in the last N days",
                sql_template="""
                SELECT user_id, age, sign_up_date, is_active,
                       CURRENT_DATE - sign_up_date::DATE as days_since_signup
                FROM users 
                WHERE sign_up_date IS NOT NULL 
                  AND sign_up_date >= CURRENT_DATE - INTERVAL '{days} days'
                ORDER BY sign_up_date::DATE DESC
                """,
                parameters=[
                    {"name": "days", "type": "integer", "default": 7, "description": "Number of days to look back"}
                ],
                category="user_analysis",
                example_usage="Find users who signed up in the last week"
            ),
            QueryTemplate(
                name="age_distribution",
                description="Age distribution analysis of users",
                sql_template="""
                SELECT 
                    CASE 
                        WHEN age < 20 THEN 'Under 20'
                        WHEN age < 30 THEN '20-29'
                        WHEN age < 40 THEN '30-39'
                        WHEN age < 50 THEN '40-49'
                        WHEN age < 60 THEN '50-59'
                        ELSE '60+'
                    END as age_group,
                    COUNT(*) as user_count,
                    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage,
                    AVG(age) as avg_age_in_group
                FROM users 
                WHERE age IS NOT NULL
                GROUP BY age_group
                ORDER BY MIN(age)
                """,
                parameters=[],
                category="analytics",
                example_usage="Analyze user age distribution by groups"
            ),
            QueryTemplate(
                name="activity_by_signup_period",
                description="User activity analysis by signup time period",
                sql_template="""
                SELECT 
                    DATE_TRUNC('{period}', sign_up_date::DATE) as signup_period,
                    COUNT(*) as total_users,
                    COUNT(CASE WHEN is_active THEN 1 END) as active_users,
                    ROUND(
                        COUNT(CASE WHEN is_active THEN 1 END) * 100.0 / COUNT(*), 
                        2
                    ) as activity_rate
                FROM users 
                WHERE sign_up_date IS NOT NULL
                GROUP BY DATE_TRUNC('{period}', sign_up_date::DATE)
                ORDER BY signup_period DESC
                LIMIT {limit}
                """,
                parameters=[
                    {"name": "period", "type": "string", "default": "month", "description": "Time period: day, week, month, year"},
                    {"name": "limit", "type": "integer", "default": 12, "description": "Number of periods to show"}
                ],
                category="analytics",
                example_usage="Analyze user activity rates by signup month"
            ),
            QueryTemplate(
                name="data_quality_check",
                description="Basic data quality assessment",
                sql_template="""
                SELECT 
                    'Total Records' as metric,
                    COUNT(*) as value,
                    '' as percentage
                FROM users
                UNION ALL
                SELECT 
                    'Missing User IDs' as metric,
                    COUNT(*) as value,
                    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM users), 2) || '%' as percentage
                FROM users WHERE user_id IS NULL
                UNION ALL
                SELECT 
                    'Missing Ages' as metric,
                    COUNT(*) as value,
                    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM users), 2) || '%' as percentage
                FROM users WHERE age IS NULL
                UNION ALL
                SELECT 
                    'Invalid Ages' as metric,
                    COUNT(*) as value,
                    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM users), 2) || '%' as percentage
                FROM users WHERE age < 0 OR age > 150
                UNION ALL
                SELECT 
                    'Missing Signup Dates' as metric,
                    COUNT(*) as value,
                    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM users), 2) || '%' as percentage
                FROM users WHERE sign_up_date IS NULL
                """,
                parameters=[],
                category="data_quality",
                example_usage="Check data quality metrics for the users table"
            )
        ]
    
    def execute_query(
        self, 
        sql_query: str, 
        use_cache: bool = True,
        max_rows: int = 10000
    ) -> QueryResult:
        """Execute SQL query with caching and analysis"""
        
        query_id = f"query_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{len(self.query_history)}"
        start_time = time.time()
        
        # Check cache first
        if use_cache:
            cached_result = self.cache.get(sql_query)
            if cached_result:
                cached_result.query_id = query_id
                self.query_history.append(cached_result)
                return cached_result
        
        # Analyze query for security and performance
        analysis = self.analyzer.analyze_query(sql_query)
        
        # Block dangerous queries
        if analysis['security_issues']:
            for issue in analysis['security_issues']:
                if issue['severity'] == 'high':
                    error_result = QueryResult(
                        query_id=query_id,
                        sql_query=sql_query,
                        result_data=None,
                        execution_time_seconds=0,
                        row_count=0,
                        column_count=0,
                        query_type=analysis['query_type'],
                        timestamp=datetime.now(),
                        error_message=f"Query blocked due to security issue: {issue['issue']}"
                    )
                    self.query_history.append(error_result)
                    return error_result
        
        try:
            # Execute query
            with self.engine.connect() as conn:
                result_proxy = conn.execute(text(sql_query))
                
                if analysis['query_type'] == QueryType.SELECT:
                    # Fetch data with row limit
                    rows = result_proxy.fetchmany(max_rows)
                    columns = result_proxy.keys()
                    
                    if rows:
                        df = pd.DataFrame(rows, columns=columns)
                    else:
                        df = pd.DataFrame()
                    
                    row_count = len(df)
                    column_count = len(df.columns) if not df.empty else 0
                    
                else:
                    # Non-SELECT queries
                    df = None
                    row_count = result_proxy.rowcount if hasattr(result_proxy, 'rowcount') else 0
                    column_count = 0
                
                execution_time = time.time() - start_time
                
                # Create result
                query_result = QueryResult(
                    query_id=query_id,
                    sql_query=sql_query,
                    result_data=df,
                    execution_time_seconds=execution_time,
                    row_count=row_count,
                    column_count=column_count,
                    query_type=analysis['query_type'],
                    timestamp=datetime.now()
                )
                
                # Cache successful SELECT queries
                if use_cache and analysis['query_type'] == QueryType.SELECT and df is not None:
                    self.cache.put(sql_query, query_result)
                
                self.query_history.append(query_result)
                
                self.logger.info(
                    f"Query executed successfully",
                    query_id=query_id,
                    execution_time=execution_time,
                    row_count=row_count
                )
                
                return query_result
                
        except Exception as e:
            execution_time = time.time() - start_time
            
            error_result = QueryResult(
                query_id=query_id,
                sql_query=sql_query,
                result_data=None,
                execution_time_seconds=execution_time,
                row_count=0,
                column_count=0,
                query_type=analysis['query_type'],
                timestamp=datetime.now(),
                error_message=str(e)
            )
            
            self.query_history.append(error_result)
            
            self.logger.error(
                f"Query execution failed",
                query_id=query_id,
                error=str(e),
                execution_time=execution_time
            )
            
            return error_result
    
    def execute_template(
        self, 
        template_name: str, 
        parameters: Dict[str, Any] = None
    ) -> QueryResult:
        """Execute a pre-built query template"""
        
        template = next((t for t in self.templates if t.name == template_name), None)
        if not template:
            raise ValueError(f"Template '{template_name}' not found")
        
        # Use default parameters if not provided
        if parameters is None:
            parameters = {}
        
        # Fill in default values for missing parameters
        for param in template.parameters:
            if param['name'] not in parameters:
                parameters[param['name']] = param['default']
        
        # Format SQL with parameters
        try:
            formatted_sql = template.sql_template.format(**parameters)
        except KeyError as e:
            raise ValueError(f"Missing required parameter: {e}")
        
        return self.execute_query(formatted_sql)
    
    def get_templates(self, category: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get available query templates"""
        templates = self.templates
        
        if category:
            templates = [t for t in templates if t.category == category]
        
        return [t.to_dict() for t in templates]
    
    def get_query_history(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Get recent query execution history"""
        recent_queries = self.query_history[-limit:] if limit else self.query_history
        return [q.to_dict() for q in reversed(recent_queries)]
    
    def get_database_schema(self) -> Dict[str, Any]:
        """Get database schema information"""
        inspector = inspect(self.engine)
        
        schema_info = {
            'tables': {},
            'total_tables': 0
        }
        
        for table_name in inspector.get_table_names():
            columns = inspector.get_columns(table_name)
            indexes = inspector.get_indexes(table_name)
            
            schema_info['tables'][table_name] = {
                'columns': [
                    {
                        'name': col['name'],
                        'type': str(col['type']),
                        'nullable': col['nullable'],
                        'default': col['default'],
                        'primary_key': col.get('primary_key', False)
                    }
                    for col in columns
                ],
                'indexes': [
                    {
                        'name': idx['name'],
                        'columns': idx['column_names'],
                        'unique': idx['unique']
                    }
                    for idx in indexes
                ],
                'column_count': len(columns)
            }
        
        schema_info['total_tables'] = len(schema_info['tables'])
        
        return schema_info
    
    def get_performance_stats(self) -> Dict[str, Any]:
        """Get SQL interface performance statistics"""
        if not self.query_history:
            return {'message': 'No queries executed yet'}
        
        successful_queries = [q for q in self.query_history if q.error_message is None]
        failed_queries = [q for q in self.query_history if q.error_message is not None]
        
        if successful_queries:
            execution_times = [q.execution_time_seconds for q in successful_queries]
            row_counts = [q.row_count for q in successful_queries]
            
            stats = {
                'total_queries': len(self.query_history),
                'successful_queries': len(successful_queries),
                'failed_queries': len(failed_queries),
                'success_rate': len(successful_queries) / len(self.query_history) * 100,
                'average_execution_time': sum(execution_times) / len(execution_times),
                'fastest_query_time': min(execution_times),
                'slowest_query_time': max(execution_times),
                'total_rows_returned': sum(row_counts),
                'average_rows_per_query': sum(row_counts) / len(successful_queries),
                'cache_stats': self.cache.get_stats(),
                'query_type_distribution': self._get_query_type_distribution()
            }
        else:
            stats = {
                'total_queries': len(self.query_history),
                'successful_queries': 0,
                'failed_queries': len(failed_queries),
                'success_rate': 0,
                'message': 'No successful queries to analyze'
            }
        
        return stats
    
    def _get_query_type_distribution(self) -> Dict[str, int]:
        """Get distribution of query types"""
        type_counts = {}
        for query in self.query_history:
            query_type = query.query_type.value
            type_counts[query_type] = type_counts.get(query_type, 0) + 1
        
        return type_counts
    
    def clear_history(self):
        """Clear query history and cache"""
        self.query_history.clear()
        self.cache.clear()
        self.logger.info("Query history and cache cleared")


# Global SQL interface instance (will be initialized with database URL)
sql_interface: Optional[SQLInterface] = None


def get_sql_interface() -> Optional[SQLInterface]:
    """Get the global SQL interface instance"""
    return sql_interface


def initialize_sql_interface(database_url: str) -> SQLInterface:
    """Initialize the global SQL interface"""
    global sql_interface
    sql_interface = SQLInterface(database_url)
    return sql_interface