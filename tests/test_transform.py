"""
tests/test_transform.py
-----------------------
Unit tests for transform_data.py using pytest.

Run:
    pytest tests/ -v
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import pandas as pd
import pytest
from transform_data import (
    transform_records,
    _validate_data,
    _add_derived_columns,
    _comfort_level,
    _heat_index,
    _wind_chill,
    dataframe_to_parquet_bytes,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def valid_record():
    return {
        "city": "London", "country": "GB",
        "latitude": 51.51, "longitude": -0.13,
        "temperature_c": 12.5, "feels_like_c": 10.2,
        "temp_min_c": 9.0, "temp_max_c": 14.0,
        "humidity_pct": 76, "pressure_hpa": 1015,
        "visibility_m": 10000, "wind_speed_ms": 5.1, "wind_deg": 270,
        "weather_main": "Clouds", "description": "overcast clouds",
        "weather_icon": "04d",
        "sunrise_utc": "2024-11-01T06:55:00+00:00",
        "sunset_utc": "2024-11-01T16:31:00+00:00",
        "observation_utc": "2024-11-01T12:00:00+00:00",
        "fetched_utc": "2024-11-01T12:01:00+00:00",
        "fetch_date": "2024-11-01", "fetch_hour": "12",
    }


@pytest.fixture
def hot_humid_record(valid_record):
    r = valid_record.copy()
    r.update({"city": "Mumbai", "temperature_c": 35.0, "humidity_pct": 85})
    return r


@pytest.fixture
def cold_windy_record(valid_record):
    r = valid_record.copy()
    r.update({"city": "Oslo", "temperature_c": -5.0, "wind_speed_ms": 10.0})
    return r


# ---------------------------------------------------------------------------
# transform_records
# ---------------------------------------------------------------------------

def test_transform_returns_dataframe(valid_record):
    df = transform_records([valid_record])
    assert df is not None
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1


def test_transform_empty_input():
    result = transform_records([])
    assert result is None


def test_transform_adds_derived_columns(valid_record):
    df = transform_records([valid_record])
    assert "comfort_level" in df.columns
    assert "heat_index_c" in df.columns
    assert "wind_chill_c" in df.columns
    assert "temp_range_c" in df.columns


def test_transform_deduplicates(valid_record):
    df = transform_records([valid_record, valid_record])
    assert len(df) == 1


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def test_validation_drops_invalid_temp(valid_record):
    bad = valid_record.copy()
    bad["temperature_c"] = 999  # impossible
    df = pd.DataFrame([bad])
    result = _validate_data(df)
    assert len(result) == 0


def test_validation_drops_invalid_humidity(valid_record):
    bad = valid_record.copy()
    bad["humidity_pct"] = 150  # > 100%
    df = pd.DataFrame([bad])
    result = _validate_data(df)
    assert len(result) == 0


def test_validation_keeps_valid(valid_record):
    df = pd.DataFrame([valid_record])
    result = _validate_data(df)
    assert len(result) == 1


# ---------------------------------------------------------------------------
# Comfort level
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("temp, expected", [
    (-10, "Freezing"),
    (5,   "Cold"),
    (15,  "Cool"),
    (22,  "Comfortable"),
    (28,  "Warm"),
    (35,  "Hot"),
])
def test_comfort_level(temp, expected):
    assert _comfort_level(temp) == expected


# ---------------------------------------------------------------------------
# Heat index
# ---------------------------------------------------------------------------

def test_heat_index_not_applied_below_27(valid_record):
    row = pd.Series(valid_record)  # temp = 12.5
    hi = _heat_index(row)
    assert hi == valid_record["temperature_c"]


def test_heat_index_applied_above_27(hot_humid_record):
    row = pd.Series(hot_humid_record)
    hi = _heat_index(row)
    assert hi > hot_humid_record["temperature_c"]  # feels hotter due to humidity


# ---------------------------------------------------------------------------
# Wind chill
# ---------------------------------------------------------------------------

def test_wind_chill_not_applied_above_10(valid_record):
    row = pd.Series(valid_record)  # temp = 12.5
    wc = _wind_chill(row)
    assert wc == valid_record["temperature_c"]


def test_wind_chill_applied_below_10(cold_windy_record):
    row = pd.Series(cold_windy_record)
    wc = _wind_chill(row)
    assert wc < cold_windy_record["temperature_c"]  # feels colder due to wind


# ---------------------------------------------------------------------------
# Parquet serialization
# ---------------------------------------------------------------------------

def test_parquet_serialization(valid_record):
    df = transform_records([valid_record])
    assert df is not None
    parquet = dataframe_to_parquet_bytes(df)
    assert isinstance(parquet, bytes)
    assert len(parquet) > 100  # non-trivial size
