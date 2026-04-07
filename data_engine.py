# data_engine.py
import pandas as pd
import duckdb
import hashlib
from config import DB_FILE, REQUIRED_SCHEMAS, PRIMARY_KEYS

def clean_volume(val):
    if isinstance(val, str):
        val = val.upper().replace(',', '')
        if 'K' in val: return float(val.replace('K', '')) * 1_000
        if 'M' in val: return float(val.replace('M', '')) * 1_000_000
    try: return float(val)
    except: return 0.0

def validate_df(df, schema_key):
    required_cols = REQUIRED_SCHEMAS[schema_key]
    missing_cols = [col for col in required_cols if col not in df.columns]
    return (False, f"Missing: {', '.join(missing_cols)}") if missing_cols else (True, "Valid")

def generate_row_hash(row):
    """Creates a unique hash for a row's content to detect changes."""
    # Convert all values to string and join them
    row_str = "".join(str(val) for val in row.values)
    return hashlib.sha256(row_str.encode()).hexdigest()

def upsert_to_duckdb(df, table_name):
    """
    Upserts data into DuckDB. 
    Inserts new records, updates changed records, ignores identical ones.
    """
    conn = duckdb.connect(DB_FILE)
    pk = PRIMARY_KEYS[table_name]
    
    # 1. Generate hash_key for the new data
    df['hash_key'] = df.apply(generate_row_hash, axis=1)
    
    # 2. Check if table exists
    table_exists = conn.execute(f"SELECT count(*) FROM information_schema.tables WHERE table_name = '{table_name}'").fetchone()[0]
    
    if not table_exists:
        # First time upload: Just save and exit
        conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")
        conn.close()
        return "Inserted as new table"

    # 3. Load existing data to compare
    existing_df = conn.execute(f"SELECT {pk}, hash_key FROM {table_name}").df()
    
    # 4. Identify rows to update or insert
    # We join the new data with the existing hashes on the Primary Key
    merged = pd.merge(df, existing_df, on=pk, how='left', suffixes=('', '_old'))
    
    # A row is "New or Changed" if:
    # - The old hash is NaN (doesn't exist)
    # - The new hash is different from the old hash
    changed_mask = (merged['hash_key_old'].isna()) | (merged['hash_key'] != merged['hash_key_old'])
    df_to_upsert = merged[changed_mask].drop(columns=['hash_key_old'])
    
    if df_to_upsert.empty:
        conn.close()
        return "No changes detected"

    # 5. Execute Upsert in DuckDB
    # First, delete the records that we are about to update/insert to avoid duplicates
    keys_to_delete = df_to_upsert[pk].tolist()
    
    # Handle different data types for the IN clause (strings vs ints)
    if isinstance(keys_to_delete[0], str):
        keys_str = ",".join([f"'{k}'" for k in keys_to_delete])
    else:
        keys_str = ",".join(map(str, keys_to_delete))
        
    conn.execute(f"DELETE FROM {table_name} WHERE {pk} IN ({keys_str})")
    
    # Insert the fresh records
    conn.execute(f"INSERT INTO {table_name} SELECT * FROM df_to_upsert")
    conn.close()
    
    return f"Upserted {len(df_to_upsert)} rows"

def load_from_db(table_name):
    """Loads data from DuckDB and ensures it is sorted by date ascending."""
    conn = duckdb.connect(DB_FILE)
    df = conn.execute(f"SELECT * FROM {table_name}").df()
    conn.close()
    
    if df.empty:
        return df

    # 1. Identify the date column for this table from config.py
    date_col = PRIMARY_KEYS.get(table_name)
    
    if date_col and date_col in df.columns:
        # 2. Convert to datetime objects to ensure correct chronological sorting
        # (This handles cases where dates are strings like '07-04-2026')
        df[date_col] = pd.to_datetime(df[date_col], dayfirst=True, errors='coerce')
        
        # 3. Sort Ascending (Oldest -> Newest)
        df = df.sort_values(by=date_col, ascending=True).reset_index(drop=True)
        
    return df