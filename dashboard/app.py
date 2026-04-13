"""
dashboard/app.py
----------------
Enhanced real-time dashboard:
  - Auto-refresh every 5 minutes
  - Hourly orders & revenue chart
  - Weather vs order correlation
  - City-level map
  - DQ results
"""

import os
import time

import pandas as pd
import psycopg2
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from streamlit_autorefresh import st_autorefresh

st.set_page_config(
    page_title="E-Commerce Order Analytics",
    page_icon="📦",
    layout="wide",
)

# 5 dakikada bir otomatik yenile (ms cinsinden)
st_autorefresh(interval=5 * 60 * 1000, key="dashboard_refresh")

DB_CONFIG = {
    "host":     os.getenv("DB_HOST", "localhost"),
    "port":     int(os.getenv("DB_PORT", 5433)),
    "dbname":   os.getenv("DB_NAME", "ecommerce"),
    "user":     os.getenv("DB_USER", "pipeline"),
    "password": os.getenv("DB_PASSWORD", "pipeline"),
}


@st.cache_data(ttl=300)
def load_daily_metrics() -> pd.DataFrame:
    conn = psycopg2.connect(**DB_CONFIG)
    df = pd.read_sql("""
        SELECT * FROM marts.mart_order_metrics
        ORDER BY order_date DESC LIMIT 90
    """, conn)
    conn.close()
    df["order_date"] = pd.to_datetime(df["order_date"])
    return df


@st.cache_data(ttl=60)
def load_hourly_metrics() -> pd.DataFrame:
    conn = psycopg2.connect(**DB_CONFIG)
    df = pd.read_sql("""
        SELECT
            DATE_TRUNC('hour', o.order_purchase_ts)     AS hour,
            COUNT(DISTINCT o.order_id)                  AS total_orders,
            ROUND(SUM(oi.price * oi.quantity)::NUMERIC, 2) AS gross_revenue,
            COUNT(DISTINCT o.customer_id)               AS unique_customers
        FROM raw.orders o
        LEFT JOIN raw.order_items oi ON oi.order_id = o.order_id
        WHERE o.order_purchase_ts >= NOW() - INTERVAL '48 hours'
        GROUP BY 1
        ORDER BY 1 DESC
    """, conn)
    conn.close()
    df["hour"] = pd.to_datetime(df["hour"])
    return df


@st.cache_data(ttl=60)
def load_weather_metrics() -> pd.DataFrame:
    conn = psycopg2.connect(**DB_CONFIG)
    df = pd.read_sql("""
        SELECT
            COALESCE(weather_condition, 'Unknown')      AS weather_condition,
            COUNT(*)                                    AS total_orders,
            ROUND(AVG(temperature_c)::NUMERIC, 1)       AS avg_temp,
            ROUND(AVG(humidity)::NUMERIC, 1)            AS avg_humidity
        FROM raw.orders
        WHERE weather_condition IS NOT NULL
          AND weather_condition != ''
          AND order_purchase_ts >= NOW() - INTERVAL '7 days'
        GROUP BY 1
        ORDER BY 2 DESC
    """, conn)
    conn.close()
    return df


@st.cache_data(ttl=60)
def load_city_metrics() -> pd.DataFrame:
    conn = psycopg2.connect(**DB_CONFIG)
    df = pd.read_sql("""
        SELECT
            c.customer_city                             AS city,
            c.country_name,
            c.country_code,
            COALESCE(c.lat, 0)                          AS lat,
            COALESCE(c.lon, 0)                          AS lon,
            COUNT(DISTINCT o.order_id)                  AS total_orders,
            ROUND(SUM(oi.price * oi.quantity)::NUMERIC, 2) AS gross_revenue
        FROM raw.customers c
        JOIN raw.orders o ON o.customer_id = c.customer_id
        LEFT JOIN raw.order_items oi ON oi.order_id = o.order_id
        WHERE c.lat IS NOT NULL AND c.lat != 0
          AND o.order_purchase_ts >= NOW() - INTERVAL '7 days'
        GROUP BY 1, 2, 3, 4, 5
        ORDER BY 6 DESC
    """, conn)
    conn.close()
    return df


@st.cache_data(ttl=60)
def load_live_feed() -> pd.DataFrame:
    conn = psycopg2.connect(**DB_CONFIG)
    df = pd.read_sql("""
        SELECT
            o.order_id,
            c.customer_city                 AS city,
            c.country_code,
            o.order_status,
            o.weather_condition,
            o.temperature_c,
            oi.price,
            o.order_purchase_ts             AS purchased_at
        FROM raw.orders o
        LEFT JOIN raw.customers c ON c.customer_id = o.customer_id
        LEFT JOIN raw.order_items oi ON oi.order_id = o.order_id
        WHERE o.order_purchase_ts >= NOW() - INTERVAL '1 hour'
        ORDER BY o.order_purchase_ts DESC
        LIMIT 20
    """, conn)
    conn.close()
    return df


@st.cache_data(ttl=300)
def load_dq_results() -> pd.DataFrame:
    conn = psycopg2.connect(**DB_CONFIG)
    df = pd.read_sql("""
        SELECT table_name, check_name, status, fail_count, run_at
        FROM raw.dq_results
        ORDER BY run_at DESC, table_name
        LIMIT 50
    """, conn)
    conn.close()
    return df


# ── Header ─────────────────────────────────────────────────────────────────────
st.title("📦 E-Commerce Order Analytics")
st.caption(f"Near real-time pipeline · dbt + Airflow + Kafka + Spark · Auto-refresh: 5 min · Last load: {pd.Timestamp.now():%Y-%m-%d %H:%M:%S}")

try:
    daily_df  = load_daily_metrics()
    hourly_df = load_hourly_metrics()
except Exception as e:
    st.error(f"Database connection error: {e}")
    st.stop()

# ── KPI Cards ──────────────────────────────────────────────────────────────────
last = daily_df.iloc[0] if not daily_df.empty else {}
prev = daily_df.iloc[1] if len(daily_df) > 1 else {}

col1, col2, col3, col4, col5 = st.columns(5)
with col1:
    delta = int(last.get("total_orders", 0)) - int(prev.get("total_orders", 0)) if len(prev) else None
    st.metric("Orders (today)", int(last.get("total_orders", 0)), delta=delta)
with col2:
    rev      = float(last.get("gross_revenue", 0))
    prev_rev = float(prev.get("gross_revenue", 0)) if len(prev) else None
    st.metric("Gross Revenue", f"${rev:,.0f}", delta=f"${rev - prev_rev:,.0f}" if prev_rev else None)
with col3:
    st.metric("Avg Order Value", f"${float(last.get('avg_order_value', 0)):,.2f}")
with col4:
    st.metric("Avg Delivery Days", f"{float(last.get('avg_delivery_days', 0) or 0):.1f}")
with col5:
    st.metric("Return Rate", f"{float(last.get('return_rate_pct', 0) or 0):.1f}%")

st.divider()

# ── Hourly Charts ──────────────────────────────────────────────────────────────
st.subheader("⏱ Hourly orders & revenue (last 48 hours)")

if not hourly_df.empty:
    hourly_sorted = hourly_df.sort_values("hour")
    col_l, col_r = st.columns(2)

    with col_l:
        fig = px.bar(
            hourly_sorted, x="hour", y="total_orders",
            color_discrete_sequence=["#4F8EF7"],
            labels={"hour": "", "total_orders": "Order count"},
        )
        fig.update_layout(margin=dict(l=0, r=0, t=10, b=0), height=260)
        st.plotly_chart(fig, use_container_width=True)

    with col_r:
        fig = px.area(
            hourly_sorted, x="hour", y="gross_revenue",
            color_discrete_sequence=["#34C78A"],
            labels={"hour": "", "gross_revenue": "Revenue ($)"},
        )
        fig.update_layout(margin=dict(l=0, r=0, t=10, b=0), height=260)
        st.plotly_chart(fig, use_container_width=True)
else:
    st.info("No hourly data yet — start the producer.")

st.divider()

# ── Weather Correlation + City Map ─────────────────────────────────────────────
col_left, col_right = st.columns(2)

with col_left:
    st.subheader("🌤 Weather condition vs order volume")
    try:
        weather_df = load_weather_metrics()
        if not weather_df.empty:
            fig = px.bar(
                weather_df, x="weather_condition", y="total_orders",
                color="avg_temp",
                color_continuous_scale="RdYlBu_r",
                labels={"weather_condition": "Weather condition", "total_orders": "Orders", "avg_temp": "Avg °C"},
            )
            fig.update_layout(margin=dict(l=0, r=0, t=10, b=0), height=300)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No weather data yet — start the hybrid producer.")
    except Exception:
        st.info("No weather data yet — start the hybrid producer.")

with col_right:
    st.subheader("🗺 City-level order map")
    try:
        city_df = load_city_metrics()
        if not city_df.empty:
            fig = px.scatter_geo(
                city_df,
                lat="lat", lon="lon",
                size="total_orders",
                color="gross_revenue",
                hover_name="city",
                hover_data={"country_name": True, "total_orders": True, "gross_revenue": True},
                color_continuous_scale="Blues",
                projection="natural earth",
                labels={"gross_revenue": "Revenue ($)", "total_orders": "Orders"},
            )
            fig.update_layout(margin=dict(l=0, r=0, t=10, b=0), height=300)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No city data yet — start the hybrid producer.")
    except Exception:
        st.info("No city data yet — start the hybrid producer.")

st.divider()

# ── Daily Revenue Trend ────────────────────────────────────────────────────────
st.subheader("📈 Daily revenue trend (last 30 days)")
if not daily_df.empty:
    chart_df = daily_df.sort_values("order_date").tail(30)
    fig = px.area(
        chart_df, x="order_date", y="gross_revenue",
        color_discrete_sequence=["#4F8EF7"],
        labels={"order_date": "", "gross_revenue": "Revenue ($)"},
    )
    fig.update_layout(margin=dict(l=0, r=0, t=10, b=0), height=220)
    st.plotly_chart(fig, use_container_width=True)

st.divider()

# ── Live Feed ──────────────────────────────────────────────────────────────────
st.subheader("⚡ Live order feed (last 1 hour)")
try:
    live_df = load_live_feed()
    if not live_df.empty:
        st.dataframe(
            live_df[["purchased_at", "city", "country_code", "order_status",
                      "weather_condition", "temperature_c", "price"]],
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.info("No orders in the last hour.")
except Exception:
    st.info("No live feed data available.")

st.divider()

# ── DQ Results ────────────────────────────────────────────────────────────────
st.subheader("✅ Latest data quality run")
try:
    dq_df = load_dq_results()
    if not dq_df.empty:
        def color_status(val):
            colors = {
                "PASS": "background-color:#d4edda",
                "WARN": "background-color:#fff3cd",
                "FAIL": "background-color:#f8d7da"
            }
            return colors.get(val, "")
        st.dataframe(
            dq_df.style.map(color_status, subset=["status"]),
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.info("No DQ results yet — run the pipeline first.")
except Exception:
    st.info("DQ results table not found.")
