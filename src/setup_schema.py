import sqlite3
import os

DB_PATH = os.path.join('..', 'data', 'processed', 'telemetry.db')

def setup_schema():
    """
    Sets up the schema for the SQLite database.
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Create tables
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS function_durations_percentiles (
        app_name TEXT,
        function_name TEXT,
        p50 REAL,
        p90 REAL,
        p95 REAL,
        p99 REAL,
        p999 REAL
    )
    """)
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS invocations_per_function_md (
        app_name TEXT,
        function_name TEXT,
        invocations REAL
    )
    """)
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS app_memory_percentiles (
        app_name TEXT,
        p50 REAL,
        p90 REAL,
        p95 REAL,
        p99 REAL,
        p999 REAL
    )
    """)
    
    conn.commit()
    conn.close()
    print("Schema setup complete.")

if __name__ == "__main__":
    setup_schema()