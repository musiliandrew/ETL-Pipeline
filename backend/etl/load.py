import pandas as pd
import psycopg2
from sqlalchemy import create_engine, text
import logging
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def get_db_config():
    """
    Load database configuration from environment variables
    
    Returns:
    --------
    dict
        Database configuration dictionary
        
    Raises:
    -------
    ValueError: If required environment variables are missing
    """
    required_vars = ['DB_HOST', 'DB_PORT', 'DB_NAME', 'DB_USER', 'DB_PASSWORD']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        raise ValueError(f"Missing required environment variables: {missing_vars}")
    
    return {
        'host': os.getenv('DB_HOST'),
        'port': int(os.getenv('DB_PORT')),
        'database': os.getenv('DB_NAME'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD')
    }


def connect_to_postgres(db_config=None):
    """
    Create connection to PostgreSQL database with retry logic
    
    Parameters:
    -----------
    db_config : dict, optional
        Database configuration with keys: host, port, database, user, password
        If None, loads from environment variables
        
    Returns:
    --------
    sqlalchemy.engine.Engine
        Database connection engine
        
    Raises:
    -------
    ConnectionError: If unable to connect after retries
    """
    # Use environment variables if no config provided
    if db_config is None:
        db_config = get_db_config()
        
    try:
        # Create connection string
        connection_string = f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
        
        # Create SQLAlchemy engine
        engine = create_engine(connection_string)
        
        # Test connection
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
            logger.info(f"Successfully connected to PostgreSQL database: {db_config['database']}")
            
        return engine
        
    except Exception as e:
        logger.error(f"Failed to connect to PostgreSQL: {str(e)}")
        raise ConnectionError(f"Database connection failed: {str(e)}")


def ensure_users_table_exists(engine):
    """
    Create users table if it doesn't exist
    
    Parameters:
    -----------
    engine : sqlalchemy.engine.Engine
        Database connection engine
        
    Returns:
    --------
    bool
        True if table exists or was created successfully
    """
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS users (
        user_id VARCHAR(50) PRIMARY KEY,
        age INTEGER NOT NULL,
        sign_up_date DATE NOT NULL,
        is_active BOOLEAN NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    
    try:
        with engine.connect() as conn:
            # Create table
            conn.execute(text(create_table_sql))
            conn.commit()
            logger.info("Users table verified/created successfully")
            return True
            
    except Exception as e:
        logger.error(f"Failed to create users table: {str(e)}")
        raise


def load_data(dataframe, db_config=None):
    """
    Load clean user data into PostgreSQL using APPEND strategy
    
    Parameters:
    -----------
    dataframe : pandas.DataFrame
        Clean DataFrame with columns: user_id, age, sign_up_date, is_active
    db_config : dict, optional
        Database configuration with keys: host, port, database, user, password
        If None, loads from environment variables
        
    Returns:
    --------
    dict
        Load results with success status and row count
    """
    
    # Validate input DataFrame
    required_columns = ['user_id', 'age', 'sign_up_date', 'is_active']
    missing_cols = [col for col in required_columns if col not in dataframe.columns]
    if missing_cols:
        raise ValueError(f"DataFrame missing required columns: {missing_cols}")
    
    logger.info(f"Starting data load for {len(dataframe)} rows")
    
    try:
        # Step 1: Connect to database
        engine = connect_to_postgres(db_config)
        
        # Step 2: Ensure table exists
        ensure_users_table_exists(engine)
        
        # Step 3: Load data using APPEND strategy
        dataframe.to_sql(
            name='users',
            con=engine,
            if_exists='append',  # Append to existing data
            index=False,  # Don't include pandas index
            method='multi'  # Faster bulk insert
        )
        
        logger.info(f"Successfully loaded {len(dataframe)} rows into users table")
        
        # Step 4: Return success result
        return {
            'success': True,
            'rows_loaded': len(dataframe),
            'message': f'Successfully loaded {len(dataframe)} rows'
        }
        
    except Exception as e:
        logger.error(f"Data load failed: {str(e)}")
        return {
            'success': False,
            'rows_loaded': 0,
            'message': f'Load failed: {str(e)}'
        }


def verify_load_success(engine, expected_rows):
    """
    Verify that data was loaded successfully into the users table
    
    Parameters:
    -----------
    engine : sqlalchemy.engine.Engine
        Database connection engine
    expected_rows : int
        Expected number of rows in the table
        
    Returns:
    --------
    dict
        Verification results with row counts and sample data
    """
    try:
        with engine.connect() as conn:
            # Count total rows
            count_result = conn.execute(text("SELECT COUNT(*) FROM users"))
            actual_rows = count_result.fetchone()[0]
            
            # Get sample data
            sample_result = conn.execute(text("SELECT * FROM users LIMIT 3"))
            sample_data = sample_result.fetchall()
            
            # Verify row count matches
            success = actual_rows == expected_rows
            
            if success:
                logger.info(f"Load verification successful: {actual_rows} rows loaded")
            else:
                logger.warning(f"Row count mismatch: expected {expected_rows}, found {actual_rows}")
            
            return {
                'success': success,
                'expected_rows': expected_rows,
                'actual_rows': actual_rows,
                'sample_data': [dict(row) for row in sample_data]
            }
            
    except Exception as e:
        logger.error(f"Load verification failed: {str(e)}")
        return {
            'success': False,
            'error': str(e)
        }


# Example usage with environment variables
if __name__ == "__main__":
    try:
        # Load database configuration from .env file
        print("Loading database configuration from .env file...")
        db_config = get_db_config()
        print(f"Connecting to database: {db_config['database']} at {db_config['host']}")
        
        # Example usage with sample data
        sample_data = pd.DataFrame({
            'user_id': ['user_001', 'user_002', 'user_003'],
            'age': [25, 35, 42],
            'sign_up_date': ['2025-01-15', '2025-01-20', '2025-01-25'],
            'is_active': [True, False, True]
        })
        
        print("Loading sample data...")
        # Using environment variables (no db_config needed)
        result = load_data(sample_data)
        
        if result['success']:
            print(f"✅ Success: {result['message']}")
            
            # Verify the load
            engine = connect_to_postgres()  # Uses env vars automatically
            verification = verify_load_success(engine, len(sample_data))
            
            if verification['success']:
                print(f"✅ Verification passed: {verification['actual_rows']} rows loaded")
                print("Sample data:", verification['sample_data'])
            else:
                print(f"⚠️ Verification failed: {verification}")
        else:
            print(f"❌ Failed: {result['message']}")
            
    except ValueError as e:
        print(f"❌ Configuration Error: {e}")
        print("Please check your .env file and ensure all required variables are set.")
    except Exception as e:
        print(f"❌ Unexpected Error: {e}")