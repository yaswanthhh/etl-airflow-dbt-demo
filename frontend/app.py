import os
import pandas as pd
import psycopg2
import streamlit as st
from streamlit_autorefresh import st_autorefresh

st_autorefresh(interval=10_000, key="data_refresh")

st.set_page_config(page_title="Listings + Weather", layout="wide")

def get_conn():
    return psycopg2.connect(
        host=os.getenv("PGHOST", "localhost"),
        port=os.getenv("PGPORT", "5432"),
        dbname=os.getenv("PGDATABASE", "warehouse"),
        user=os.getenv("PGUSER", "demo"),
        password=os.getenv("PGPASSWORD", "demo"),
    )

@st.cache_data(ttl=10)
def load_fact():
    sql = """
    select
      ts_hour,
      location,
      listings_cnt,
      avg_price_sek,
      temperature_2m,
      precipitation,
      wind_speed_10m,
      weather_code
    from public.fact_listings_weather_hourly
    order by ts_hour desc, location
    """
    with get_conn() as conn:
        return pd.read_sql(sql, conn)

st.title("Vehicle listings vs weather (hourly)")

df = load_fact()

locations = sorted(df["location"].dropna().unique().tolist())
picked = st.multiselect("Locations", locations, default=locations)

df2 = df[df["location"].isin(picked)].copy()

st.subheader("Latest rows")
st.dataframe(df2.head(30), use_container_width=True)

st.subheader("Avg price over time")
pivot = (
    df2.pivot_table(index="ts_hour", columns="location", values="avg_price_sek", aggfunc="mean")
    .sort_index()
)
st.line_chart(pivot)
