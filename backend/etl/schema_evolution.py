"""
Schema Evolution Handler
========================

Handles changing data structures, column additions/removals, 
and automatic schema migrations for ETL pipelines.
"""

import json
import pandas as pd
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
from sqlalchemy import create_engine, text, MetaData, Table, Column, String, Integer, Boolean, Date, DateTime
from sqlalchemy.exc import OperationalError
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class SchemaRegistry:
    """
    Manages schema versions and evolution history
    """
    
    def __init__(self, registry_path: str = "schemas/registry.json"):
        self.registry_path = Path(registry_path)
        self.registry_path.parent.mkdir(parents=True, exist_ok=True)
        self.schemas = self._load_registry()
        
    def _load_registry(self) -> Dict[str, Any]:
        """Load schema registry from file"""
        if self.registry_path.exists():
            try:
                with open(self.registry_path, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logger.error(f"Failed to load schema registry: {str(e)}")
        
        # Default registry structure
        return {
            "current_version": 1,
            "schemas": {
                "1": {
                    "version": 1,
                    "created_at": datetime.now().isoformat(),
                    "description": "Initial schema",
                    "columns": {
                        "user_id": {"type": "string", "required": True, "max_length": 50},
                        "age": {"type": "integer", "required": True, "min_value": 0, "max_value": 150},
                        "sign_up_date": {"type": "date", "required": True},
                        "is_active": {"type": "boolean", "required": True}
                    },
                    "primary_key": "user_id",
                    "table_name": "users"
                }
            },
            "evolution_history": []
        }
    
    def _save_registry(self):
        """Save schema registry to file"""
        try:
            with open(self.registry_path, 'w') as f:
                json.dump(self.schemas, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save schema registry: {str(e)}")
    
    def get_current_schema(self) -> Dict[str, Any]:
        """Get the current schema version"""
        current_version = str(self.schemas["current_version"])
        return self.schemas["schemas"].get(current_version, {})
    
    def add_schema_version(self, new_schema: Dict[str, Any], description: str = "") -> int:
        """Add a new schema version"""
        new_version = self.schemas["current_version"] + 1
        
        new_schema_entry = {
            "version": new_version,
            "created_at": datetime.now().isoformat(),
            "description": description,
            **new_schema
        }
        
        self.schemas["schemas"][str(new_version)] = new_schema_entry
        self.schemas["current_version"] = new_version
        
        # Record evolution
        self.schemas["evolution_history"].append({
            "from_version": new_version - 1,
            "to_version": new_version,
            "timestamp": datetime.now().isoformat(),
            "description": description
        })
        
        self._save_registry()
        logger.info(f"Added schema version {new_version}: {description}")
        return new_version
    
    def get_schema_diff(self, version1: int, version2: int) -> Dict[str, Any]:
        """Compare two schema versions"""
        schema1 = self.schemas["schemas"].get(str(version1), {})
        schema2 = self.schemas["schemas"].get(str(version2), {})
        
        if not schema1 or not schema2:
            return {"error": "One or both schema versions not found"}
        
        columns1 = schema1.get("columns", {})
        columns2 = schema2.get("columns", {})
        
        added_columns = set(columns2.keys()) - set(columns1.keys())
        removed_columns = set(columns1.keys()) - set(columns2.keys())
        common_columns = set(columns1.keys()) & set(columns2.keys())
        
        modified_columns = {}
        for col in common_columns:
            if columns1[col] != columns2[col]:
                modified_columns[col] = {
                    "old": columns1[col],
                    "new": columns2[col]
                }
        
        return {
            "added_columns": list(added_columns),
            "removed_columns": list(removed_columns),
            "modified_columns": modified_columns,
            "version_diff": f"{version1} → {version2}"
        }


class SchemaDetector:
    """
    Detects schema from incoming data and identifies changes
    """
    
    def __init__(self):
        self.type_mapping = {
            'int64': 'integer',
            'float64': 'float', 
            'object': 'string',
            'bool': 'boolean',
            'datetime64[ns]': 'datetime'
        }
    
    def detect_schema_from_dataframe(self, df: pd.DataFrame, table_name: str = "users") -> Dict[str, Any]:
        """Detect schema from pandas DataFrame"""
        columns = {}
        
        for col_name, dtype in df.dtypes.items():
            col_info = {
                "type": self.type_mapping.get(str(dtype), "string"),
                "required": not df[col_name].isnull().any(),
                "null_count": int(df[col_name].isnull().sum()),
                "unique_count": int(df[col_name].nunique())
            }
            
            # Add type-specific metadata
            if col_info["type"] == "string":
                col_info["max_length"] = int(df[col_name].astype(str).str.len().max()) if not df[col_name].empty else 0
            elif col_info["type"] == "integer":
                col_info["min_value"] = int(df[col_name].min()) if not df[col_name].empty else 0
                col_info["max_value"] = int(df[col_name].max()) if not df[col_name].empty else 0
            elif col_info["type"] == "float":
                col_info["min_value"] = float(df[col_name].min()) if not df[col_name].empty else 0.0
                col_info["max_value"] = float(df[col_name].max()) if not df[col_name].empty else 0.0
            
            columns[col_name] = col_info
        
        return {
            "columns": columns,
            "table_name": table_name,
            "row_count": len(df),
            "detected_at": datetime.now().isoformat()
        }
    
    def compare_with_current_schema(self, detected_schema: Dict[str, Any], registry: SchemaRegistry) -> Dict[str, Any]:
        """Compare detected schema with current schema"""
        current_schema = registry.get_current_schema()
        current_columns = current_schema.get("columns", {})
        detected_columns = detected_schema.get("columns", {})
        
        # Find differences
        new_columns = set(detected_columns.keys()) - set(current_columns.keys())
        missing_columns = set(current_columns.keys()) - set(detected_columns.keys())
        
        schema_changes = []
        compatibility_issues = []
        
        # Check new columns
        for col in new_columns:
            schema_changes.append({
                "type": "column_added",
                "column": col,
                "details": detected_columns[col]
            })
        
        # Check missing columns
        for col in missing_columns:
            if current_columns[col].get("required", False):
                compatibility_issues.append({
                    "type": "required_column_missing",
                    "column": col,
                    "severity": "high"
                })
            else:
                schema_changes.append({
                    "type": "column_removed",
                    "column": col
                })
        
        # Check type changes in existing columns
        common_columns = set(current_columns.keys()) & set(detected_columns.keys())
        for col in common_columns:
            current_type = current_columns[col].get("type")
            detected_type = detected_columns[col].get("type")
            
            if current_type != detected_type:
                compatibility_issues.append({
                    "type": "type_mismatch",
                    "column": col,
                    "current_type": current_type,
                    "detected_type": detected_type,
                    "severity": "medium"
                })
        
        return {
            "schema_changes": schema_changes,
            "compatibility_issues": compatibility_issues,
            "is_compatible": len(compatibility_issues) == 0,
            "requires_migration": len(schema_changes) > 0 or len(compatibility_issues) > 0
        }


class DatabaseMigrator:
    """
    Handles database schema migrations
    """
    
    def __init__(self, engine=None):
        self.engine = engine
        self.migration_history_table = "schema_migrations"
        
    def ensure_migration_table(self):
        """Create migration history table if it doesn't exist"""
        try:
            with self.engine.connect() as conn:
                create_sql = f"""
                CREATE TABLE IF NOT EXISTS {self.migration_history_table} (
                    id SERIAL PRIMARY KEY,
                    version INTEGER NOT NULL,
                    description TEXT,
                    applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    rollback_sql TEXT
                );
                """
                conn.execute(text(create_sql))
                conn.commit()
        except Exception as e:
            logger.error(f"Failed to create migration table: {str(e)}")
            raise
    
    def apply_schema_changes(self, schema_changes: List[Dict[str, Any]], target_table: str = "users") -> bool:
        """Apply schema changes to database"""
        if not schema_changes:
            logger.info("No schema changes to apply")
            return True
            
        self.ensure_migration_table()
        migration_sql = []
        rollback_sql = []
        
        try:
            with self.engine.connect() as conn:
                for change in schema_changes:
                    change_type = change["type"]
                    column = change["column"]
                    
                    if change_type == "column_added":
                        col_details = change["details"]
                        sql_type = self._map_type_to_sql(col_details["type"], col_details)
                        nullable = "NULL" if not col_details.get("required", False) else "NOT NULL"
                        
                        add_sql = f"ALTER TABLE {target_table} ADD COLUMN {column} {sql_type} {nullable};"
                        drop_sql = f"ALTER TABLE {target_table} DROP COLUMN {column};"
                        
                        migration_sql.append(add_sql)
                        rollback_sql.insert(0, drop_sql)  # Reverse order for rollback
                        
                        logger.info(f"Adding column: {column}")
                        conn.execute(text(add_sql))
                    
                    elif change_type == "column_removed":
                        # Be cautious with column removal - usually just mark as deprecated
                        logger.warning(f"Column removal requested for {column} - skipping for safety")
                        continue
                
                # Record migration
                migration_record = f"""
                INSERT INTO {self.migration_history_table} (version, description, rollback_sql)
                VALUES ({datetime.now().strftime('%Y%m%d%H%M%S')}, 
                        'Auto-migration: {len(schema_changes)} changes', 
                        '{"; ".join(rollback_sql)}');
                """
                conn.execute(text(migration_record))
                conn.commit()
                
                logger.info(f"Successfully applied {len(schema_changes)} schema changes")
                return True
                
        except Exception as e:
            logger.error(f"Schema migration failed: {str(e)}")
            return False
    
    def _map_type_to_sql(self, col_type: str, col_details: Dict[str, Any]) -> str:
        """Map column type to SQL type"""
        type_mapping = {
            'string': f"VARCHAR({col_details.get('max_length', 255)})",
            'integer': 'INTEGER',
            'float': 'FLOAT',
            'boolean': 'BOOLEAN',
            'date': 'DATE',
            'datetime': 'TIMESTAMP'
        }
        return type_mapping.get(col_type, 'TEXT')


class SchemaEvolutionManager:
    """
    Main class that orchestrates schema evolution
    """
    
    def __init__(self, registry_path: str = "schemas/registry.json"):
        self.registry = SchemaRegistry(registry_path)
        self.detector = SchemaDetector()
        self.migrator = None  # Will be initialized with database engine
        
    def process_data_with_schema_evolution(self, df: pd.DataFrame, engine=None) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """
        Process data with automatic schema evolution
        
        Returns:
        --------
        Tuple[pd.DataFrame, Dict[str, Any]]
            (processed_dataframe, evolution_report)
        """
        evolution_report = {
            "schema_changed": False,
            "migration_applied": False,
            "compatibility_issues": [],
            "actions_taken": []
        }
        
        try:
            # Detect schema from incoming data
            detected_schema = self.detector.detect_schema_from_dataframe(df)
            
            # Compare with current schema
            comparison = self.detector.compare_with_current_schema(detected_schema, self.registry)
            
            if comparison["requires_migration"]:
                evolution_report["schema_changed"] = True
                evolution_report["compatibility_issues"] = comparison["compatibility_issues"]
                
                # Handle compatibility issues
                if not comparison["is_compatible"]:
                    logger.warning("Schema compatibility issues detected")
                    df = self._handle_compatibility_issues(df, comparison["compatibility_issues"])
                    evolution_report["actions_taken"].append("Applied compatibility fixes")
                
                # Apply schema changes if we have a database engine
                if engine and comparison["schema_changes"]:
                    self.migrator = DatabaseMigrator(engine)
                    if self.migrator.apply_schema_changes(comparison["schema_changes"]):
                        evolution_report["migration_applied"] = True
                        evolution_report["actions_taken"].append("Applied database migration")
                
                # Update schema registry
                if comparison["schema_changes"]:
                    description = f"Auto-evolution: {len(comparison['schema_changes'])} changes"
                    new_version = self.registry.add_schema_version(detected_schema, description)
                    evolution_report["actions_taken"].append(f"Updated schema to version {new_version}")
                    
            return df, evolution_report
            
        except Exception as e:
            logger.error(f"Schema evolution failed: {str(e)}")
            evolution_report["error"] = str(e)
            return df, evolution_report
    
    def _handle_compatibility_issues(self, df: pd.DataFrame, issues: List[Dict[str, Any]]) -> pd.DataFrame:
        """Handle compatibility issues by modifying the DataFrame"""
        for issue in issues:
            issue_type = issue["type"]
            column = issue["column"]
            
            if issue_type == "required_column_missing":
                # Add missing required column with default values
                if column == "user_id":
                    df[column] = range(1, len(df) + 1)  # Generate IDs
                elif column == "age":
                    df[column] = 0  # Default age
                elif column == "is_active":
                    df[column] = True  # Default to active
                elif column == "sign_up_date":
                    df[column] = datetime.now().date()  # Default to today
                
                logger.info(f"Added missing required column '{column}' with default values")
            
            elif issue_type == "type_mismatch":
                # Try to convert types
                current_type = issue["current_type"]
                detected_type = issue["detected_type"]
                
                try:
                    if current_type == "integer" and detected_type == "string":
                        df[column] = pd.to_numeric(df[column], errors='coerce').fillna(0).astype(int)
                    elif current_type == "boolean" and detected_type == "string":
                        df[column] = df[column].map({'true': True, 'false': False, '1': True, '0': False})
                    elif current_type == "date" and detected_type == "string":
                        df[column] = pd.to_datetime(df[column], errors='coerce').dt.date
                    
                    logger.info(f"Converted column '{column}' from {detected_type} to {current_type}")
                except Exception as e:
                    logger.warning(f"Failed to convert column '{column}': {str(e)}")
        
        return df


# Example usage and testing
if __name__ == "__main__":
    print("Schema Evolution Manager - Test")
    print("=" * 50)
    
    # Initialize schema evolution manager
    evolution_manager = SchemaEvolutionManager()
    
    # Create test data with schema changes
    test_data = pd.DataFrame({
        'user_id': [1, 2, 3],
        'age': [25, 30, 35],
        'sign_up_date': ['2024-01-01', '2024-01-02', '2024-01-03'],
        'is_active': [True, False, True],
        'email': ['test1@email.com', 'test2@email.com', 'test3@email.com'],  # New column
        'subscription_tier': ['basic', 'premium', 'basic']  # Another new column
    })
    
    print(f"\n📊 Test data shape: {test_data.shape}")
    print(f"Columns: {list(test_data.columns)}")
    
    # Test schema detection
    print("\n🔍 Detecting schema...")
    detected_schema = evolution_manager.detector.detect_schema_from_dataframe(test_data)
    print(f"Detected {len(detected_schema['columns'])} columns")
    
    # Test schema comparison
    print("\n🔄 Checking for schema changes...")
    comparison = evolution_manager.detector.compare_with_current_schema(detected_schema, evolution_manager.registry)
    
    if comparison["requires_migration"]:
        print(f"✅ Schema changes detected:")
        for change in comparison["schema_changes"]:
            print(f"   - {change['type']}: {change['column']}")
    else:
        print("✅ No schema changes required")
    
    # Test evolution process
    print("\n🚀 Testing schema evolution...")
    processed_df, evolution_report = evolution_manager.process_data_with_schema_evolution(test_data)
    
    print(f"Schema changed: {evolution_report['schema_changed']}")
    print(f"Actions taken: {evolution_report['actions_taken']}")
    
    # Show current schema version
    current_schema = evolution_manager.registry.get_current_schema()
    print(f"\nCurrent schema version: {current_schema.get('version', 'unknown')}")
    print(f"Schema columns: {list(current_schema.get('columns', {}).keys())}")