"""
dashboard.py
------------
Streamlit dashboard for the AWS Weather Data Pipeline.

Displays:
  - Current weather metrics (latest readings per city)
  - Temperature comparison chart (bar chart)
  - Humidity comparison chart
  - Daily trend line chart (multi-city)
  - Weather condition distribution (pie/bar)

Run:
    streamlit run dashboard.py

The dashboard queries AWS Athena live. If Athena is not configured,
it falls back to a local CSV demo dataset so you can still see the UI.
"""

import logging
import os
import sys
from datetime import datetime, timezone
import pandas as pd
import streamlit as st

# Allow imports from src/ when running from project root
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

from config import setup_logging, CITIES

logger = logging.getLogger(__name__)
setup_logging()

# ── Try to import Athena query runner (may fail without AWS credentials) ──
try:
    from athena_query import run_named_query, QUERIES
    ATHENA_AVAILABLE = True
except Exception as exc:
    logger.warning(f"Athena import failed ({exc}). Running in demo mode.")
    ATHENA_AVAILABLE = False


# ---------------------------------------------------------------------------
# Page configuration
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="Weather Data Pipeline — AWS",
    page_icon="🌤️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# CSS overrides for a cleaner look
# ---------------------------------------------------------------------------

st.markdown("""
<style>
    .metric-card {
        background-color: #f0f8ff;
        border-radius: 10px;
        padding: 12px 16px;
        border-left: 4px solid #1a73e8;
        margin-bottom: 8px;
    }
    .section-header {
        font-size: 1.1rem;
        font-weight: 600;
        color: #1a73e8;
        margin-top: 1.5rem;
        margin-bottom: 0.5rem;
    }
    .stMetric > div { border-radius: 8px; }
</style>
""", unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# Demo / fallback data generator
# ---------------------------------------------------------------------------

def get_demo_latest() -> pd.DataFrame:
    """Generate synthetic latest-reading data for demo mode."""
    import random
    random.seed(42)
    cities_data = [
        ("London",    "GB",  12.5, 76, 5.1, "Clouds"),
        ("New York",  "US",  18.2, 62, 3.8, "Clear"),
        ("Tokyo",     "JP",  22.0, 58, 2.5, "Clear"),
        ("Sydney",    "AU",  25.1, 70, 4.2, "Sunny"),
        ("Mumbai",    "IN",  31.5, 82, 6.0, "Haze"),
    ]
    rows = []
    for city, country, temp, hum, wind, cond in cities_data:
        rows.append({
            "city": city, "country": country,
            "temperature_c": temp + random.uniform(-1, 1),
            "humidity_pct": hum,
            "wind_speed_ms": wind,
            "pressure_hpa": 1013,
            "feels_like_c": temp - 2,
            "description": cond,
            "comfort_level": "Comfortable" if temp < 25 else "Warm",
            "fetch_date": datetime.utcnow().strftime("%Y-%m-%d"),
        })
    return pd.DataFrame(rows)


def get_demo_trend() -> pd.DataFrame:
    """Generate synthetic daily trend data for demo mode."""
    import random
    random.seed(99)
    rows = []
    cities = ["London", "New York", "Tokyo", "Sydney", "Mumbai"]
    base_temps = [10, 17, 21, 24, 30]
    from datetime import timedelta
    today = datetime.utcnow().date()
    for i in range(7):
        date = (today - timedelta(days=6 - i)).strftime("%Y-%m-%d")
        for city, base in zip(cities, base_temps):
            rows.append({
                "fetch_date": date,
                "city": city,
                "avg_temp_c":      round(base + random.uniform(-2, 2), 1),
                "avg_humidity_pct": round(60 + random.uniform(-10, 10), 1),
                "avg_wind_ms":     round(4 + random.uniform(-1, 1), 1),
            })
    return pd.DataFrame(rows)


def get_demo_humidity() -> pd.DataFrame:
    return pd.DataFrame({
        "city":         ["London", "New York", "Tokyo", "Sydney", "Mumbai"],
        "avg_humidity": [76, 62, 58, 70, 82],
        "min_humidity": [55, 45, 40, 55, 70],
        "max_humidity": [92, 78, 75, 85, 95],
    })


# ---------------------------------------------------------------------------
# Data loading (with caching)
# ---------------------------------------------------------------------------

@st.cache_data(ttl=300)   # cache for 5 minutes
def load_latest_readings() -> pd.DataFrame:
    if ATHENA_AVAILABLE:
        try:
            return run_named_query("latest_readings")
        except Exception as e:
            st.warning(f"Athena error: {e}. Showing demo data.")
    return get_demo_latest()


@st.cache_data(ttl=300)
def load_daily_trend() -> pd.DataFrame:
    if ATHENA_AVAILABLE:
        try:
            return run_named_query("daily_trend")
        except Exception as e:
            st.warning(f"Athena error: {e}. Showing demo data.")
    return get_demo_trend()


@st.cache_data(ttl=300)
def load_humidity() -> pd.DataFrame:
    if ATHENA_AVAILABLE:
        try:
            return run_named_query("humidity_analysis")
        except Exception as e:
            st.warning(f"Athena error: {e}. Showing demo data.")
    return get_demo_humidity()


@st.cache_data(ttl=300)
def load_condition_distribution() -> pd.DataFrame:
    if ATHENA_AVAILABLE:
        try:
            return run_named_query("condition_distribution")
        except Exception as e:
            pass
    # Demo fallback
    return pd.DataFrame({
        "weather_main": ["Clear", "Clouds", "Rain", "Haze", "Drizzle"],
        "occurrence_count": [45, 30, 15, 7, 3],
    })


# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------

with st.sidebar:
    st.image("https://openweathermap.org/themes/openweathermap/assets/img/logo_white_cropped.png", width=160)
    st.title("⚙️ Controls")

    st.markdown("**Data Source**")
    mode = "🟢 Live (Athena)" if ATHENA_AVAILABLE else "🟡 Demo Mode"
    st.info(mode)

    st.markdown("**Refresh**")
    if st.button("🔄 Refresh Data"):
        st.cache_data.clear()
        st.rerun()

    st.markdown("**Filter Cities**")
    df_latest_sidebar = load_latest_readings()
    all_cities = sorted(df_latest_sidebar["city"].unique().tolist()) if not df_latest_sidebar.empty else []
    selected_cities = st.multiselect("Select cities:", all_cities, default=all_cities)

    st.markdown("---")
    st.markdown("**AWS Stack**")
    st.markdown("- 🪣 S3 Storage\n- 🔍 Glue Crawler\n- 📊 Athena SQL\n- 🐍 Python + Pandas")
    st.markdown("---")
    st.caption(f"Last refreshed: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")

# ---------------------------------------------------------------------------
# Main layout
# ---------------------------------------------------------------------------

st.title("🌤️ Real-Time Weather Data Pipeline")
st.markdown("*End-to-end AWS pipeline: OpenWeather API → S3 → Glue → Athena → Streamlit*")

# ── Load data ────────────────────────────────────────────────────────────────
df_latest    = load_latest_readings()
df_trend     = load_daily_trend()
df_humidity  = load_humidity()
df_condition = load_condition_distribution()

# Apply city filter
if selected_cities:
    df_latest   = df_latest[df_latest["city"].isin(selected_cities)]
    df_trend    = df_trend[df_trend["city"].isin(selected_cities)]
    df_humidity = df_humidity[df_humidity["city"].isin(selected_cities)]


# ── Section 1: Current Metrics ───────────────────────────────────────────────
st.markdown('<p class="section-header">📍 Current Weather Metrics</p>', unsafe_allow_html=True)

if df_latest.empty:
    st.warning("No data available. Make sure you have run the pipeline at least once.")
else:
    cols = st.columns(min(len(df_latest), 5))
    for i, (_, row) in enumerate(df_latest.iterrows()):
        if i >= len(cols):
            break
        with cols[i]:
            temp = round(float(row.get("temperature_c", 0)), 1)
            feels = round(float(row.get("feels_like_c", 0)), 1)
            delta = round(temp - feels, 1)
            st.metric(
                label=f"🏙️ {row['city']}, {row.get('country', '')}",
                value=f"{temp}°C",
                delta=f"Feels like {feels}°C",
            )
            st.caption(f"💧 {row.get('humidity_pct', '-')}%  |  💨 {row.get('wind_speed_ms', '-')} m/s")
            comfort = row.get("comfort_level", row.get("description", ""))
            st.caption(f"☁️ {comfort}")

st.divider()


# ── Section 2: Temperature Bar Chart ─────────────────────────────────────────
col1, col2 = st.columns(2)

with col1:
    st.markdown('<p class="section-header">🌡️ Temperature by City</p>', unsafe_allow_html=True)
    if not df_latest.empty:
        temp_df = df_latest[["city", "temperature_c", "feels_like_c"]].copy()
        temp_df = temp_df.rename(columns={
            "temperature_c": "Actual (°C)",
            "feels_like_c":  "Feels Like (°C)",
        })
        temp_df = temp_df.set_index("city")
        st.bar_chart(temp_df, height=300)
    else:
        st.info("No temperature data.")

with col2:
    st.markdown('<p class="section-header">💧 Humidity by City</p>', unsafe_allow_html=True)
    if not df_humidity.empty:
        hum_df = df_humidity.copy()
        # Support both Athena and demo schemas
        val_col = "avg_humidity" if "avg_humidity" in hum_df.columns else "avg_humidity_pct"
        hum_df = hum_df[["city", val_col]].set_index("city").rename(columns={val_col: "Avg Humidity (%)"})
        st.bar_chart(hum_df, height=300)
    else:
        st.info("No humidity data.")

st.divider()


# ── Section 3: Daily Temperature Trend ───────────────────────────────────────
st.markdown('<p class="section-header">📈 Daily Temperature Trend (7 Days)</p>', unsafe_allow_html=True)

if not df_trend.empty:
    try:
        trend_pivot = df_trend.pivot_table(
            index="fetch_date", columns="city", values="avg_temp_c", aggfunc="mean"
        )
        trend_pivot.index = pd.to_datetime(trend_pivot.index)
        trend_pivot = trend_pivot.sort_index()
        st.line_chart(trend_pivot, height=320)
    except Exception as e:
        st.warning(f"Could not render trend chart: {e}")
        st.dataframe(df_trend)
else:
    st.info("Trend data not yet available. Run the pipeline for at least 2 days.")

st.divider()


# ── Section 4: Humidity Trend ─────────────────────────────────────────────────
st.markdown('<p class="section-header">💦 Daily Humidity Trend</p>', unsafe_allow_html=True)

if not df_trend.empty and "avg_humidity_pct" in df_trend.columns:
    try:
        hum_pivot = df_trend.pivot_table(
            index="fetch_date", columns="city", values="avg_humidity_pct", aggfunc="mean"
        )
        hum_pivot.index = pd.to_datetime(hum_pivot.index)
        hum_pivot = hum_pivot.sort_index()
        st.area_chart(hum_pivot, height=280)
    except Exception as e:
        st.warning(f"Could not render humidity chart: {e}")
else:
    st.info("Historical humidity data will appear after multiple pipeline runs.")

st.divider()


# ── Section 5: Weather Conditions + Raw Data ──────────────────────────────────
col3, col4 = st.columns([1, 2])

with col3:
    st.markdown('<p class="section-header">☁️ Condition Distribution</p>', unsafe_allow_html=True)
    if not df_condition.empty:
        st.dataframe(
            df_condition.head(10),
            use_container_width=True,
            hide_index=True,
        )

with col4:
    st.markdown('<p class="section-header">🗂️ Latest Raw Readings</p>', unsafe_allow_html=True)
    if not df_latest.empty:
        display_cols = [c for c in [
            "city", "country", "temperature_c", "feels_like_c",
            "humidity_pct", "wind_speed_ms", "description", "fetch_date",
        ] if c in df_latest.columns]
        st.dataframe(
            df_latest[display_cols].reset_index(drop=True),
            use_container_width=True,
            hide_index=True,
        )


# ── Footer ────────────────────────────────────────────────────────────────────
st.divider()
st.markdown(
    "<small>🔧 Built with Python · Pandas · Boto3 · AWS S3 · Glue · Athena · Streamlit</small>",
    unsafe_allow_html=True,
)
