# data_engine.py
import re
import pandas as pd
import duckdb
import hashlib
from config import DB_FILE, REQUIRED_SCHEMAS, PRIMARY_KEYS

def normalize_columns(df):
    """Normalize DataFrame column names to lowercase_underscore format."""
    def _norm(col):
        col = col.strip()
        col = col.replace(' ', '_')
        col = re.sub(r'[^a-zA-Z0-9_]', '_', col)
        col = re.sub(r'_+', '_', col)
        col = col.strip('_')
        col = col.lower()
        return col
    df.columns = [_norm(c) for c in df.columns]
    return df

def standardize_dates(df, date_columns=None):
    """Standardize all date columns to yyyy-mm-dd format. NaN dates are kept as NaN (not converted to 'nan' string)."""
    if date_columns is None:
        date_columns = ['date', 'release_date', 'as_of_date_in_form_yymmdd']
    
    for col in date_columns:
        if col in df.columns:
            try:
                dt_series = pd.to_datetime(df[col], errors='coerce')
                df[col] = dt_series.dt.strftime('%Y-%m-%d')
                df.loc[dt_series.isna(), col] = pd.NA
            except Exception:
                pass
    
    return df

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
    row_str = "".join(str(val) for val in row.values)
    return hashlib.sha256(row_str.encode()).hexdigest()

def upsert_to_duckdb(df, table_name):
    """
    Upserts data into DuckDB.
    Inserts new records, updates changed records, ignores identical ones.
    Standardizes all dates to yyyy-mm-dd format before storing.
    """
    df = df.copy()
    df = standardize_dates(df)
    pk = PRIMARY_KEYS[table_name]
    df = df.dropna(subset=[pk])

    if df.empty:
        return "No valid data to upsert (all rows have invalid keys)"

    conn = duckdb.connect(DB_FILE)
    df['hash_key'] = df.apply(generate_row_hash, axis=1)

    table_exists = conn.execute(f"SELECT count(*) FROM information_schema.tables WHERE table_name = '{table_name}'").fetchone()[0]

    if not table_exists:
        conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")
        conn.close()
        return "Inserted as new table"

    table_cols = conn.execute(f"PRAGMA table_info('{table_name}')").df()['name'].tolist()

    if 'hash_key' not in table_cols:
        conn.execute(f"ALTER TABLE {table_name} ADD COLUMN hash_key VARCHAR")
        table_cols.append('hash_key')

    existing_df = conn.execute(f"SELECT {pk}, hash_key FROM {table_name}").df()

    merged = pd.merge(df, existing_df, on=pk, how='left', suffixes=('', '_old'))
    changed_mask = (merged['hash_key_old'].isna()) | (merged['hash_key'] != merged['hash_key_old'])
    df_to_upsert = merged[changed_mask].drop(columns=['hash_key_old'])

    if df_to_upsert.empty:
        conn.close()
        return "No changes detected"

    for col in table_cols:
        if col not in df_to_upsert.columns:
            df_to_upsert[col] = pd.NA

    df_to_upsert = df_to_upsert[table_cols]

    keys_to_delete = df_to_upsert[pk].tolist()

    if isinstance(keys_to_delete[0], str):
        keys_str = ",".join([f"'{k}'" for k in keys_to_delete])
    else:
        keys_str = ",".join(map(str, keys_to_delete))

    conn.execute(f"DELETE FROM {table_name} WHERE {pk} IN ({keys_str})")

    quoted_cols = ", ".join([f'"{c}"' for c in table_cols])
    conn.register("df_to_upsert_aligned", df_to_upsert)
    conn.execute(f"INSERT INTO {table_name} ({quoted_cols}) SELECT {quoted_cols} FROM df_to_upsert_aligned")

    conn.close()
    return f"Upserted {len(df_to_upsert)} rows"

def load_from_db(table_name):
    """Loads data from DuckDB, ensures dates are in yyyy-mm-dd format, and sorts by date ascending."""
    conn = duckdb.connect(DB_FILE)
    df = conn.execute(f"SELECT * FROM {table_name}").df()
    conn.close()

    if df.empty:
        return df

    date_col = PRIMARY_KEYS.get(table_name)

    if date_col and date_col in df.columns:
        df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
        df[date_col] = df[date_col].dt.strftime('%Y-%m-%d')
        df = df.sort_values(by=date_col, ascending=True).reset_index(drop=True)

    return df