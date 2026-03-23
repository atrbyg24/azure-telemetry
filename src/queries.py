import streamlit as st
import pandas as pd
from pathlib import Path
import sqlite3


@st.cache_resource
def get_connection():
    base_path = Path(__file__).parent.parent.absolute()
    db_path = base_path / "data" / "processed" / "telemetry.db"
    
    if not db_path.exists():
        db_path = base_path / "telemetry.db"
        
    if not db_path.exists():
        st.error(f"Database not found! Tried: {db_path}")
        st.stop()
        
    return sqlite3.connect(str(db_path), check_same_thread=False)


@st.cache_data
def fetch_fleet_demand(day_str: str):
    query = f'SELECT * FROM "invocations_per_function_md.anon.d{day_str}" LIMIT 20'
    df = pd.read_sql(query, get_connection())
    if 'hashapp' in df.columns:
        df['hashapp'] = df['hashapp'].astype(str).str[:8]
    return df


@st.cache_data
def fetch_latency_outliers(day_str: str):
    query = f"""
    SELECT hashapp, average as avg_ms, percentile_average_99 as p99_ms
    FROM "function_durations_percentiles.anon.d{day_str}"
    ORDER BY p99_ms DESC LIMIT 10
    """
    df = pd.read_sql(query, get_connection())
    if 'hashapp' in df.columns:
        df['hashapp'] = df['hashapp'].astype(str).str[:8]
    return df


@st.cache_data
def fetch_memory_audit(day_str: str):
    query = f'SELECT hashapp, averageallocatedmb FROM "app_memory_percentiles.anon.d{day_str}" LIMIT 50'
    df = pd.read_sql(query, get_connection())
    if 'hashapp' in df.columns:
        df['hashapp'] = df['hashapp'].astype(str).str[:8]
    return df