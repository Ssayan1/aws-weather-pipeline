"""
transform_data.py
-----------------
Transforms raw weather records (list of dicts) into a clean, validated
Pandas DataFrame ready for upload to S3 as Parquet.

Responsibilities:
  - Schema enforcement (correct dtypes)
  - Null / outlier validation
  - Derived columns (heat index, wind chill, comfort level)
  - Deduplication
  - Partitioned Parquet output

Usage:
    from transform_data import transform_records
    df = transform_records(raw_records)
"""

import logging
import os
from datetime import datetime, timezone
from io import BytesIO
from typing import Optional

import pandas as pd
import numpy as np

from config import setup_logging

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Schema definition
# ---------------------------------------------------------------------------

EXPECTED_COLUMNS = {
    "city":            "string",
    "country":         "string",
    "latitude":        "float64",
    "longitude":       "float64",
    "temperature_c":   "float64",
    "feels_like_c":    "float64",
    "temp_min_c":      "float64",
    "temp_max_c":      "float64",
    "humidity_pct":    "int64",
    "pressure_hpa":    "int64",
    "visibility_m":    "int64",
    "wind_speed_ms":   "float64",
    "wind_deg":        "int64",
    "weather_main":    "string",
    "description":     "string",
    "weather_icon":    "string",
    "sunrise_utc":     "string",
    "sunset_utc":      "string",
    "observation_utc": "string",
    "fetched_utc":     "string",
    "fetch_date":      "string",
    "fetch_hour":      "string",
}

# Reasonable bounds for validation
VALIDATION_RULES = {
    "temperature_c":  (-90, 60),
    "humidity_pct":   (0, 100),
    "pressure_hpa":   (870, 1085),
    "wind_speed_ms":  (0, 120),
}


# ---------------------------------------------------------------------------
# Main transformation function
# ---------------------------------------------------------------------------

def transform_records(records: list[dict]) -> Optional[pd.DataFrame]:
    """
    Transform a list of raw weather dicts into a clean Pandas DataFrame.

    Steps:
      1. Create DataFrame from records
      2. Enforce expected schema / dtypes
      3. Validate data quality (drop or flag bad rows)
      4. Add derived / computed columns
      5. Deduplicate on (city, fetch_date, fetch_hour)

    Returns:
        Cleaned DataFrame, or None if input is empty / all records invalid.
    """
    if not records:
        logger.warning("transform_records called with empty record list.")
        return None

    logger.info(f"Transforming {len(records)} raw records…")
    df = pd.DataFrame(records)

    # ── 1. Enforce schema ────────────────────────────────────────────────────
    df = _enforce_schema(df)

    # ── 2. Validate data quality ─────────────────────────────────────────────
    df = _validate_data(df)
    if df.empty:
        logger.error("All records were dropped during validation.")
        return None

    # ── 3. Derive computed columns ───────────────────────────────────────────
    df = _add_derived_columns(df)

    # ── 4. Deduplicate ───────────────────────────────────────────────────────
    before = len(df)
    df = df.drop_duplicates(subset=["city", "fetch_date", "fetch_hour"], keep="last")
    after = len(df)
    if before != after:
        logger.info(f"Dropped {before - after} duplicate rows.")

    # ── 5. Sort for readability ──────────────────────────────────────────────
    df = df.sort_values(["fetch_date", "fetch_hour", "city"]).reset_index(drop=True)

    logger.info(f"Transformation complete: {len(df)} clean records, {len(df.columns)} columns.")
    return df


# ---------------------------------------------------------------------------
# Schema enforcement
# ---------------------------------------------------------------------------

def _enforce_schema(df: pd.DataFrame) -> pd.DataFrame:
    """Cast each column to its declared dtype; add missing columns as null."""
    for col, dtype in EXPECTED_COLUMNS.items():
        if col not in df.columns:
            logger.warning(f"Column '{col}' missing — filling with NaN.")
            df[col] = pd.NA

        try:
            if dtype in ("float64",):
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")
            elif dtype in ("int64",):
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64").astype("Int64")
            elif dtype == "string":
                df[col] = df[col].astype(str).replace("nan", pd.NA).astype("string")
        except Exception as exc:
            logger.warning(f"Could not cast column '{col}' to {dtype}: {exc}")

    return df


# ---------------------------------------------------------------------------
# Data validation
# ---------------------------------------------------------------------------

def _validate_data(df: pd.DataFrame) -> pd.DataFrame:
    """Drop rows that violate physical / logical constraints."""
    initial_count = len(df)

    for col, (low, high) in VALIDATION_RULES.items():
        if col in df.columns:
            mask = df[col].between(low, high) | df[col].isna()
            bad = (~mask).sum()
            if bad:
                logger.warning(
                    f"Dropping {bad} row(s) where '{col}' is outside [{low}, {high}]."
                )
            df = df[mask]

    # City name must be present
    df = df[df["city"].notna() & (df["city"] != "")]

    dropped = initial_count - len(df)
    if dropped:
        logger.info(f"Validation dropped {dropped}/{initial_count} rows.")

    return df.reset_index(drop=True)


# ---------------------------------------------------------------------------
# Derived columns
# ---------------------------------------------------------------------------

def _add_derived_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add business-useful computed columns.

    Derived columns:
      - heat_index_c   : apparent temperature accounting for humidity (Steadman formula)
      - wind_chill_c   : apparent temperature factoring in wind (at low temps)
      - comfort_level  : categorical label (Freezing / Cold / Cool / Comfortable / Warm / Hot)
      - is_daytime     : boolean, True if observation is between sunrise and sunset
      - temp_range_c   : daily temp range (max - min)
      - pipeline_ts    : ISO timestamp this row was processed
    """
    # Heat index (only meaningful above 27°C and humidity > 40%)
    df["heat_index_c"] = df.apply(_heat_index, axis=1).round(2)

    # Wind chill (only meaningful below 10°C and wind > 1.3 m/s)
    df["wind_chill_c"] = df.apply(_wind_chill, axis=1).round(2)

    # Comfort level category
    df["comfort_level"] = df["temperature_c"].apply(_comfort_level)

    # Temperature range
    df["temp_range_c"] = (df["temp_max_c"] - df["temp_min_c"]).round(2)

    # Pipeline processing timestamp
    df["pipeline_ts"] = datetime.now(timezone.utc).isoformat()

    return df


def _heat_index(row: pd.Series) -> float:
    """Steadman heat index. Returns temperature_c unchanged outside valid range."""
    t = row.get("temperature_c", 0)
    h = row.get("humidity_pct", 0)
    if pd.isna(t) or pd.isna(h) or t < 27 or h < 40:
        return t
    # Simplified Steadman formula (metric)
    hi = (-8.78469475556
          + 1.61139411    * t
          + 2.33854883889 * h
          - 0.14611605    * t * h
          - 0.012308094   * t**2
          - 0.016424828   * h**2
          + 0.002211732   * t**2 * h
          + 0.00072546    * t * h**2
          - 0.000003582   * t**2 * h**2)
    return round(hi, 2)


def _wind_chill(row: pd.Series) -> float:
    """Wind chill index. Returns temperature_c unchanged outside valid range."""
    t = row.get("temperature_c", 0)
    v = row.get("wind_speed_ms", 0)
    if pd.isna(t) or pd.isna(v) or t > 10 or v < 1.3:
        return t
    # Environment Canada formula
    v_kmh = v * 3.6
    wc = (13.12 + 0.6215 * t
          - 11.37 * v_kmh**0.16
          + 0.3965 * t * v_kmh**0.16)
    return round(wc, 2)


def _comfort_level(temp: float) -> str:
    """Map temperature (°C) to a human-readable comfort label."""
    if pd.isna(temp):
        return "Unknown"
    if temp < 0:
        return "Freezing"
    elif temp < 10:
        return "Cold"
    elif temp < 18:
        return "Cool"
    elif temp < 25:
        return "Comfortable"
    elif temp < 32:
        return "Warm"
    else:
        return "Hot"


# ---------------------------------------------------------------------------
# Parquet serialization
# ---------------------------------------------------------------------------

def dataframe_to_parquet_bytes(df: pd.DataFrame) -> bytes:
    """
    Serialize a DataFrame to Parquet bytes (in-memory).
    Drops partition columns (fetch_date, fetch_hour) because they
    already exist in the S3 folder path — keeping them inside the
    Parquet file causes Glue to register duplicate columns.
    """
    buf = BytesIO()
    partition_cols = ["fetch_date", "fetch_hour"]
    cols_to_drop = [c for c in partition_cols if c in df.columns]
    if cols_to_drop:
        df = df.drop(columns=cols_to_drop)
    df.to_parquet(buf, index=False, engine="pyarrow", compression="snappy")
    return buf.getvalue()


def save_locally(df: pd.DataFrame, output_dir: str = "data") -> str:
    """
    Save the DataFrame as a local Parquet file (useful for testing
    without AWS credentials).

    Returns the path of the saved file.
    """
    os.makedirs(output_dir, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    path = os.path.join(output_dir, f"weather_{ts}.parquet")
    df.to_parquet(path, index=False, engine="pyarrow", compression="snappy")
    logger.info(f"Data saved locally to: {path}")
    return path


# ---------------------------------------------------------------------------
# CLI / Quick test
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    setup_logging()

    # Synthetic test record matching the schema from fetch_weather.py
    sample = [
        {
            "city": "London", "country": "GB",
            "latitude": 51.51, "longitude": -0.13,
            "temperature_c": 12.5, "feels_like_c": 10.2,
            "temp_min_c": 9.0, "temp_max_c": 14.0,
            "humidity_pct": 76, "pressure_hpa": 1015,
            "visibility_m": 10000, "wind_speed_ms": 5.1, "wind_deg": 270,
            "weather_main": "Clouds", "description": "overcast clouds",
            "weather_icon": "04d",
            "sunrise_utc": "2024-11-01T06:55:00+00:00",
            "sunset_utc":  "2024-11-01T16:31:00+00:00",
            "observation_utc": "2024-11-01T12:00:00+00:00",
            "fetched_utc":     "2024-11-01T12:01:00+00:00",
            "fetch_date": "2024-11-01", "fetch_hour": "12",
        }
    ]

    df = transform_records(sample)
    if df is not None:
        print(df.T)
        save_locally(df, output_dir="/tmp/weather_test")
