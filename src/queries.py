import streamlit as st
import pandas as pd

@st.cache_data
def fetch_fleet_demand(day_str: str):
    query = f'SELECT * FROM "invocations_per_function_md.anon.d{day_str}" LIMIT 10'
    return pd.read_sql(query, get_connection())

@st.cache_data
def fetch_latency_outliers(day_str: str):
    query = f"""
    SELECT hashapp, average as avg_ms, percentile_average_99 as p99_ms
    FROM "function_durations_percentiles.anon.d{day_str}"
    ORDER BY p99_ms DESC LIMIT 10
    """
    return pd.read_sql(query, get_connection())

@st.cache_data
def fetch_memory_audit(day_str: str):
    query = f'SELECT hashapp, averageallocatedmb FROM "app_memory_percentiles.anon.d{day_str}" LIMIT 50'
    return pd.read_sql(query, get_connection())