import streamlit as st
import pandas as pd
import plotly.express as px
import os

st.set_page_config(
    page_title="Traffic Violation Dashboard",
    page_icon="🚦",
    layout="wide"
)

@st.cache_data
def load_data(parquet_path):
    """
    Safely loads a Parquet file/directory into a Pandas DataFrame.
    """
    if not os.path.exists(parquet_path):
        st.error(f"Data file not found: {parquet_path}. Please run the analysis pipeline first.")
        return None
    try:
        df = pd.read_parquet(parquet_path)
        return df
    except Exception as e:
        st.error(f"Error loading data from {parquet_path}: {e}")
        return None

ANALYSIS_PATH = "traffic_analysis_results"
ADVANCED_PATH = "advanced_traffic_analysis"

hourly_path = os.path.join(ANALYSIS_PATH, "hourly_counts.parquet")
dayofweek_path = os.path.join(ANALYSIS_PATH, "dayofweek_counts.parquet")
type_path = os.path.join(ANALYSIS_PATH, "type_counts.parquet")
top_locations_path = os.path.join(ANALYSIS_PATH, "top_10_locations.parquet")
hotspots_path = os.path.join(ADVANCED_PATH, "significant_hotspots.parquet")

df_hourly = load_data(hourly_path)
df_dayofweek = load_data(dayofweek_path)
df_type = load_data(type_path)
df_top_locations = load_data(top_locations_path)
df_hotspots = load_data(hotspots_path)


st.title("🚦 Smart Traffic Violation Pattern Analysis")

st.markdown("""
This dashboard visualizes traffic violation patterns identified by the PySpark pipeline. 
The data is processed from source, cleaned, aggregated, and then displayed here.
""")

st.header("Overall Summary")
col1, col2, col3 = st.columns(3)

if df_type is not None and not df_type.empty:
    total_violations = int(df_type['Total_Violations'].sum())
    common_violation = df_type.iloc[0]['Violation_Type']
    col1.metric("Total Violations", f"{total_violations:,}")
    col2.metric("Most Common Violation", common_violation)
else:
    col1.metric("Total Violations", "N/A")
    col2.metric("Most Common Violation", "N/A")

if df_top_locations is not None and not df_top_locations.empty:
    top_location = df_top_locations.iloc[0]['Location']
    col3.metric("Top Hotspot Location", top_location)
else:
    col3.metric("Top Hotspot Location", "N/A")

st.header("Time-Based Patterns")
col1, col2 = st.columns(2)

if df_hourly is not None and not df_hourly.empty:
    max_hourly_val = df_hourly['Total_Violations'].max() 
    fig_hourly = px.bar(
        df_hourly.sort_values('Violation_Hour'), 
        x="Violation_Hour", 
        y="Total_Violations", 
        title="Violations by Hour of Day",
        labels={"Violation_Hour": "Hour (0-23)", "Total_Violations": "Total Violations"}
    )
    fig_hourly.update_xaxes(type='category')
    fig_hourly.update_yaxes(range=[0, max_hourly_val + (max_hourly_val * 0.1) + 1]) 
    col1.plotly_chart(fig_hourly, use_container_width=True)
else:
    col1.subheader("Hourly Data")
    col1.write("Hourly data not available.")

if df_dayofweek is not None and not df_dayofweek.empty:
    max_day_val = df_dayofweek['Total_Violations'].max() 
    day_order = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    df_dayofweek['Violation_DayOfWeek'] = pd.Categorical(
        df_dayofweek['Violation_DayOfWeek'], categories=day_order, ordered=True
    )
    df_dayofweek = df_dayofweek.sort_values('Violation_DayOfWeek')
    
    fig_day = px.bar(
        df_dayofweek, 
        x="Violation_DayOfWeek", 
        y="Total_Violations", 
        title="Violations by Day of Week",
        labels={"Violation_DayOfWeek": "Day of Week", "Total_Violations": "Total Violations"}
    )
    fig_day.update_yaxes(range=[0, max_day_val + (max_day_val * 0.1) + 1])
    col2.plotly_chart(fig_day, use_container_width=True)
else:
    col2.subheader("Day of Week Data")
    col2.write("Day of week data not available.")

st.header("Violation & Location Analysis")
col1, col2 = st.columns(2)

# Violation Types
if df_type is not None and not df_type.empty:
    max_type_val = df_type['Total_Violations'].max()
    fig_type = px.bar(
        df_type.sort_values('Total_Violations', ascending=False), 
        x="Violation_Type", 
        y="Total_Violations", 
        title="Top Violation Types",
        labels={"Violation_Type": "Violation Type", "Total_Violations": "Total Violations"}
    )
    fig_type.update_yaxes(range=[0, max_type_val + (max_type_val * 0.1) + 1]) 
    col1.plotly_chart(fig_type, use_container_width=True)
else:
    col1.subheader("Violation Type Data")
    col1.write("Violation type data not available.")

if df_top_locations is not None and not df_top_locations.empty:
    max_loc_val = df_top_locations['Total_Violations'].max() 
    fig_loc = px.bar(
        df_top_locations.sort_values('Total_Violations', ascending=False), 
        x="Location", 
        y="Total_Violations", 
        title=f"Top {len(df_top_locations)} Violation Locations",
        labels={"Location": "Location ID", "Total_Violations": "Total Violations"}
    )
    fig_loc.update_yaxes(range=[0, max_loc_val + (max_loc_val * 0.1) + 1])
    col2.plotly_chart(fig_loc, use_container_width=True)
else:
    col2.subheader("Top Location Data")
    col2.write("Top location data not available.")

st.header("Data Explorer")
col1, col2 = st.columns(2)
with col1:
    st.subheader("Statistical Hotspots")
    if df_hotspots is not None:
        st.dataframe(df_hotspots)
    else:
        st.write("Hotspot data not available.")

with col2:
    st.subheader("Top Violation Types (Raw)")
    if df_type is not None:
        st.dataframe(df_type)
    else:
        st.write("Type data not available.")
