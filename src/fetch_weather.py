"""
fetch_weather.py
----------------
Fetches current weather data from the OpenWeather API for a list of cities.

Usage:
    python src/fetch_weather.py              # fetch all configured cities
    python src/fetch_weather.py --city Paris # fetch a single city

Returns:
    List[dict] — one record per city, written to logs and returned to caller.
"""

import argparse
import json
import logging
import time
from datetime import datetime, timezone
from typing import Any, Optional

import requests

from config import (
    OPENWEATHER_API_KEY,
    OPENWEATHER_BASE_URL,
    CITIES,
    UNITS,
    validate_config,
    setup_logging,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Core fetch function
# ---------------------------------------------------------------------------

def fetch_weather_for_city(city: str, retries: int = 3, backoff: float = 2.0) -> Optional[dict]:
    """
    Fetch current weather for a single city from OpenWeather API.

    Args:
        city    : City name (e.g. "London" or "London,GB")
        retries : Number of retry attempts on transient failures
        backoff : Seconds to wait between retries (doubles each attempt)

    Returns:
        Parsed weather record dict, or None on failure.
    """
    url = f"{OPENWEATHER_BASE_URL}/weather"
    params = {
        "q":     city,
        "appid": OPENWEATHER_API_KEY,
        "units": UNITS,
    }

    attempt = 0
    wait = backoff

    while attempt < retries:
        try:
            logger.debug(f"Fetching weather for '{city}' (attempt {attempt + 1}/{retries})")
            response = requests.get(url, params=params, timeout=10)

            if response.status_code == 401:
                logger.error("Invalid API key — check OPENWEATHER_API_KEY in .env")
                return None

            if response.status_code == 404:
                logger.warning(f"City not found: '{city}'")
                return None

            if response.status_code == 429:
                logger.warning(f"Rate limit hit for '{city}'. Waiting {wait}s…")
                time.sleep(wait)
                wait *= 2
                attempt += 1
                continue

            response.raise_for_status()
            raw = response.json()
            record = _parse_weather_response(raw)
            logger.info(f"Fetched weather for {city}: {record['temperature_c']}°C, {record['description']}")
            return record

        except requests.exceptions.ConnectionError:
            logger.error(f"Network error while fetching '{city}'")
        except requests.exceptions.Timeout:
            logger.error(f"Timeout while fetching '{city}'")
        except requests.exceptions.RequestException as exc:
            logger.error(f"Unexpected error for '{city}': {exc}")

        attempt += 1
        if attempt < retries:
            logger.info(f"Retrying in {wait}s…")
            time.sleep(wait)
            wait *= 2

    logger.error(f"All {retries} attempts failed for city '{city}'")
    return None


def _parse_weather_response(raw: dict[str, Any]) -> dict:
    """
    Flatten the nested OpenWeather JSON into a clean, flat record.

    Input (raw API response excerpt):
        {
          "name": "London",
          "main": {"temp": 15.2, "humidity": 72, "pressure": 1012, ...},
          "weather": [{"description": "overcast clouds", "main": "Clouds"}],
          "wind": {"speed": 5.1},
          "sys": {"country": "GB", "sunrise": ..., "sunset": ...},
          "visibility": 10000,
          "dt": 1700000000
        }

    Returns flat dict ready for Pandas / Parquet.
    """
    now_utc = datetime.now(timezone.utc)
    weather_info = raw.get("weather", [{}])[0]
    main = raw.get("main", {})
    wind = raw.get("wind", {})
    sys_info = raw.get("sys", {})
    coord = raw.get("coord", {})

    return {
        # Identifiers
        "city":             raw.get("name", "unknown"),
        "country":          sys_info.get("country", ""),
        "latitude":         coord.get("lat"),
        "longitude":        coord.get("lon"),
        # Temperature
        "temperature_c":    round(main.get("temp", 0), 2),
        "feels_like_c":     round(main.get("feels_like", 0), 2),
        "temp_min_c":       round(main.get("temp_min", 0), 2),
        "temp_max_c":       round(main.get("temp_max", 0), 2),
        # Atmospheric
        "humidity_pct":     main.get("humidity", 0),
        "pressure_hpa":     main.get("pressure", 0),
        "visibility_m":     raw.get("visibility", 0),
        # Wind
        "wind_speed_ms":    round(wind.get("speed", 0), 2),
        "wind_deg":         wind.get("deg", 0),
        # Conditions
        "weather_main":     weather_info.get("main", ""),
        "description":      weather_info.get("description", ""),
        "weather_icon":     weather_info.get("icon", ""),
        # Sun
        "sunrise_utc":      datetime.fromtimestamp(sys_info.get("sunrise", 0), tz=timezone.utc).isoformat(),
        "sunset_utc":       datetime.fromtimestamp(sys_info.get("sunset", 0), tz=timezone.utc).isoformat(),
        # Timestamps
        "observation_utc":  datetime.fromtimestamp(raw.get("dt", 0), tz=timezone.utc).isoformat(),
        "fetched_utc":      now_utc.isoformat(),
        "fetch_date":       now_utc.strftime("%Y-%m-%d"),       # partition key
        "fetch_hour":       now_utc.strftime("%H"),             # partition key
    }


# ---------------------------------------------------------------------------
# Batch fetch
# ---------------------------------------------------------------------------

def fetch_all_cities(cities: list[str] | None = None) -> list[dict]:
    """
    Fetch weather for all configured cities (or override with custom list).

    Returns:
        List of successfully fetched weather records.
    """
    target_cities = cities or CITIES
    records: list[dict] = []

    logger.info(f"Starting batch fetch for {len(target_cities)} cities…")

    for city in target_cities:
        record = fetch_weather_for_city(city)
        if record:
            records.append(record)
        # Polite delay to respect rate limits (free tier = 60 req/min)
        time.sleep(0.2)

    logger.info(f"Batch fetch complete. Success: {len(records)}/{len(target_cities)}")
    return records


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    setup_logging()
    validate_config()

    parser = argparse.ArgumentParser(description="Fetch weather data from OpenWeather API")
    parser.add_argument("--city", type=str, help="Fetch a single city by name")
    parser.add_argument("--output", type=str, help="Save results to a JSON file")
    args = parser.parse_args()

    if args.city:
        results = [r for r in [fetch_weather_for_city(args.city)] if r]
    else:
        results = fetch_all_cities()

    if args.output and results:
        with open(args.output, "w") as f:
            json.dump(results, f, indent=2)
        logger.info(f"Results saved to {args.output}")
    else:
        print(json.dumps(results, indent=2))
