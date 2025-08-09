import pandas as pd


def transform(dataFrame):
    # Make a copy to avoid modifying the original DataFrame
    df = dataFrame.copy()
    
    # Handle column name variations
    # Standardize signup_date → sign_up_date
    if 'signup_date' in df.columns and 'sign_up_date' not in df.columns:
        df = df.rename(columns={'signup_date': 'sign_up_date'})
    
    # Required columns for analyst queries
    required_columns = ['user_id', 'age', 'sign_up_date', 'is_active']
    
    # Check if required columns exist
    missing_cols = [col for col in required_columns if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")
    
    # Transform age: convert to integer, handle floats
    if 'age' in df.columns:
        df['age'] = pd.to_numeric(df['age'], errors='coerce')  # Convert to numeric, NaN for invalid
        df['age'] = df['age'].fillna(0).astype(int)  # Fill NaN with 0, convert to int
    
    # Transform is_active: convert 1/0 to True/False
    if 'is_active' in df.columns:
        df['is_active'] = df['is_active'].map({1: True, 0: False, '1': True, '0': False, 
                                              True: True, False: False, 'true': True, 'false': False})
        df['is_active'] = df['is_active'].fillna(False)  # Default to False for unclear values
    
    # Transform sign_up_date: standardize to YYYY-MM-DD format
    if 'sign_up_date' in df.columns:
        df['sign_up_date'] = pd.to_datetime(df['sign_up_date'], errors='coerce')
        df['sign_up_date'] = df['sign_up_date'].dt.strftime('%Y-%m-%d')
    
    # Ensure user_id is preserved as string/int
    if 'user_id' in df.columns:
        df['user_id'] = df['user_id'].astype(str)
    
    # Select only the required columns in the right order
    ready_df = df[required_columns]
    
    return ready_df