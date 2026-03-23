import streamlit as st
import pandas as pd
import sqlite3
import plotly.express as px
from pathlib import Path
from queries import fetch_fleet_demand, fetch_latency_outliers, fetch_memory_audit

st.set_page_config(page_title="Azure 2019 Telemetry Explorer", layout="wide")
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


def main():
    st.title("Azure 2019 Telemetry Explorer")
    
    st.sidebar.header("Filter Settings")
    day_selected = st.sidebar.slider("Select Trace Day", 1, 14, 1)
    day_str = str(day_selected).zfill(2)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader(f"📈 Fleet Demand (Day {day_str})")
        df_invoc = fetch_fleet_demand(day_str)
        
        df_long = df_invoc.melt(id_vars=['hashapp'], var_name='minute', value_name='count')
        df_long['minute'] = pd.to_numeric(df_long['minute'], errors='coerce')
        fig_demand = px.line(df_long.dropna(), x='minute', y='count', color='hashapp', 
                             template="plotly_dark")
        st.plotly_chart(fig_demand, use_container_width=True)
    
    with col2:
        st.subheader("⚠️ High Latency Outliers")
        df_lat = fetch_latency_outliers(day_str)
        fig_lat = px.bar(df_lat, x='hashapp', y='p99_ms', color='avg_ms',
                         labels={'p99_ms': 'Tail Latency (ms)'})
        st.plotly_chart(fig_lat, use_container_width=True)
    
    st.divider()
    st.subheader("Resource Efficiency Audit")
    if day_selected <= 12:
        df_mem = fetch_memory_audit(day_str)
        st.dataframe(df_mem, use_container_width=True)
    else:
        st.warning("Memory telemetry unavailable for Days 13-14.")

if __name__ == "__main__":
    main()