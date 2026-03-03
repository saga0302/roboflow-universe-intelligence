import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import os

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'roboflow_warehouse.duckdb')

st.set_page_config(
    page_title="Roboflow Universe Intelligence",
    layout="wide"
)

@st.cache_data
def load_data():
    conn = duckdb.connect(DB_PATH, read_only=True)
    staged = conn.execute("SELECT * FROM main.stg_datasets").df()
    growth = conn.execute("SELECT * FROM main.dataset_growth").df()
    top    = conn.execute("SELECT * FROM main.top_categories").df()
    conn.close()
    return staged, growth, top

staged, growth, top = load_data()

# Header
st.title("Roboflow Universe Intelligence Platform")
st.caption("Self-serve analytics on Roboflow's public dataset ecosystem · Powered by dbt + DuckDB")
st.divider()

# KPI Metrics
col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Datasets",      f"{len(staged):,}")
col2.metric("Total Images",        f"{staged['image_count'].sum():,.0f}")
col3.metric("Avg Classes/Dataset", f"{staged['class_count'].mean():.1f}")
col4.metric("Unique Workspaces",   f"{staged['workspace'].nunique()}")

st.divider()

# Row 1: Two charts side by side
col_l, col_r = st.columns(2)

with col_l:
    st.subheader("Dataset Size Distribution")
    size_counts = staged['dataset_size_bucket'].value_counts().reset_index()
    size_counts.columns = ['Size Bucket', 'Count']
    fig1 = px.pie(
        size_counts, values='Count', names='Size Bucket',
        color_discrete_sequence=px.colors.sequential.Purples_r,
        hole=0.4
    )
    fig1.update_layout(margin=dict(t=20, b=20))
    st.plotly_chart(fig1, use_container_width=True)

with col_r:
    st.subheader("Classification Complexity")
    comp_counts = staged['classification_complexity'].value_counts().reset_index()
    comp_counts.columns = ['Complexity', 'Count']
    fig2 = px.bar(
        comp_counts, x='Complexity', y='Count',
        color='Complexity',
        color_discrete_sequence=px.colors.sequential.Teal
    )
    fig2.update_layout(showlegend=False, margin=dict(t=20, b=20))
    st.plotly_chart(fig2, use_container_width=True)

st.divider()

# Row 2: Top datasets by image count
st.subheader("Top Datasets by Image Count")
fig3 = px.bar(
    top.head(10),
    x='image_count', y='dataset_name',
    orientation='h',
    color='image_count',
    color_continuous_scale='Viridis',
    labels={'image_count': 'Images', 'dataset_name': 'Dataset'}
)
fig3.update_layout(yaxis={'categoryorder': 'total ascending'}, margin=dict(t=20))
st.plotly_chart(fig3, use_container_width=True)

st.divider()

# Row 3: Growth analytics
st.subheader("Dataset Growth Analytics")
col_a, col_b = st.columns(2)

with col_a:
    fig4 = px.scatter(
        growth,
        x='avg_images', y='total_datasets',
        size='total_images',
        color='dataset_size_bucket',
        hover_data=['dataset_type', 'classification_complexity'],
        title='Avg Images vs Total Datasets',
        color_discrete_sequence=px.colors.qualitative.Bold
    )
    st.plotly_chart(fig4, use_container_width=True)

with col_b:
    st.dataframe(
        growth[['dataset_type', 'dataset_size_bucket',
                'classification_complexity', 'total_datasets',
                'avg_images', 'total_annotations']]
        .sort_values('total_datasets', ascending=False)
        .reset_index(drop=True),
        use_container_width=True,
        height=350
    )

st.divider()

# Raw data explorer
with st.expander("Raw Dataset Explorer"):
    search = st.text_input("Search datasets by name")
    filtered = staged[staged['dataset_name'].str.contains(search, case=False)] if search else staged
    st.dataframe(filtered, use_container_width=True)

st.caption("Built by Sagarika Raju · Data pipeline: Roboflow API → DuckDB → dbt → Streamlit")