"""
athena_query.py
---------------
Execute SQL queries against AWS Athena and return results as Pandas DataFrames.

How Athena works (important for beginners):
  1. You submit a query → Athena returns an execution_id immediately.
  2. Athena runs the query asynchronously in the background.
  3. You poll the execution status until it reaches SUCCEEDED or FAILED.
  4. You fetch the results (CSV) from S3 and parse them.

This module handles all four steps.

Usage:
    from athena_query import run_query, QUERIES
    df = run_query(QUERIES["avg_temperature"])
"""

import logging
import time
from typing import Optional

import boto3
import pandas as pd
from botocore.exceptions import ClientError

from config import (
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    AWS_REGION,
    ATHENA_DATABASE,
    S3_ATHENA_RESULTS,
    validate_config,
    setup_logging,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Pre-built analytical queries
# ---------------------------------------------------------------------------

QUERIES = {
    # 1. Average temperature per city (all time)
    "avg_temperature": f"""
        SELECT
            city,
            country,
            ROUND(AVG(temperature_c), 2)   AS avg_temp_c,
            ROUND(AVG(feels_like_c), 2)    AS avg_feels_like_c,
            ROUND(AVG(humidity_pct), 1)    AS avg_humidity_pct,
            COUNT(*)                        AS observation_count
        FROM {ATHENA_DATABASE}.weather
        GROUP BY city, country
        ORDER BY avg_temp_c DESC;
    """,

    # 2. Maximum temperature recorded per city
    "max_temperature": f"""
        SELECT
            city,
            country,
            MAX(temperature_c)              AS max_temp_c,
            MAX(temp_max_c)                 AS max_daily_high_c,
            fetch_date
        FROM {ATHENA_DATABASE}.weather
        GROUP BY city, country, fetch_date
        ORDER BY max_temp_c DESC
        LIMIT 20;
    """,

    # 3. Daily weather trend (average per city per day)
    "daily_trend": f"""
        SELECT
            fetch_date,
            city,
            ROUND(AVG(temperature_c), 2)   AS avg_temp_c,
            ROUND(MIN(temp_min_c), 2)       AS min_temp_c,
            ROUND(MAX(temp_max_c), 2)       AS max_temp_c,
            ROUND(AVG(humidity_pct), 1)     AS avg_humidity_pct,
            ROUND(AVG(wind_speed_ms), 2)    AS avg_wind_ms
        FROM {ATHENA_DATABASE}.weather
        GROUP BY fetch_date, city
        ORDER BY fetch_date ASC, city ASC;
    """,

    # 4. Humidity analysis by city
    "humidity_analysis": f"""
        SELECT
            city,
            ROUND(AVG(humidity_pct), 1)     AS avg_humidity,
            MIN(humidity_pct)               AS min_humidity,
            MAX(humidity_pct)               AS max_humidity,
            ROUND(STDDEV(humidity_pct), 2)  AS stddev_humidity,
            COUNT(*)                        AS records
        FROM {ATHENA_DATABASE}.weather
        GROUP BY city
        ORDER BY avg_humidity DESC;
    """,

    # 5. Latest reading per city (most recent observation)
    "latest_readings": f"""
        SELECT *
        FROM {ATHENA_DATABASE}.weather
        WHERE (city, fetched_utc) IN (
            SELECT city, MAX(fetched_utc)
            FROM {ATHENA_DATABASE}.weather
            GROUP BY city
        )
        ORDER BY city;
    """,

    # 6. Weather condition distribution
    "condition_distribution": f"""
        SELECT
            weather_main,
            description,
            COUNT(*) AS occurrence_count,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct_of_total
        FROM {ATHENA_DATABASE}.weather
        GROUP BY weather_main, description
        ORDER BY occurrence_count DESC;
    """,

    # 7. Hourly temperature profile (heatmap data)
    "hourly_profile": f"""
        SELECT
            city,
            fetch_hour,
            ROUND(AVG(temperature_c), 2) AS avg_temp_c,
            COUNT(*) AS observations
        FROM {ATHENA_DATABASE}.weather
        GROUP BY city, fetch_hour
        ORDER BY city, CAST(fetch_hour AS INTEGER);
    """,

    # 8. Wind analysis
    "wind_analysis": f"""
        SELECT
            city,
            ROUND(AVG(wind_speed_ms), 2)    AS avg_wind_ms,
            ROUND(MAX(wind_speed_ms), 2)    AS max_wind_ms,
            ROUND(AVG(wind_speed_ms) * 3.6, 2) AS avg_wind_kmh
        FROM {ATHENA_DATABASE}.weather
        GROUP BY city
        ORDER BY avg_wind_ms DESC;
    """,
}


# ---------------------------------------------------------------------------
# Athena client factory
# ---------------------------------------------------------------------------

def get_athena_client():
    """Return a configured Boto3 Athena client."""
    kwargs = {"region_name": AWS_REGION}
    if AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY:
        kwargs["aws_access_key_id"]     = AWS_ACCESS_KEY_ID
        kwargs["aws_secret_access_key"] = AWS_SECRET_ACCESS_KEY
    return boto3.client("athena", **kwargs)


# ---------------------------------------------------------------------------
# Query execution
# ---------------------------------------------------------------------------

def start_query(client, sql: str, database: str, output_location: str) -> Optional[str]:
    """
    Submit a query to Athena and return the execution ID.

    Args:
        client          : Boto3 Athena client
        sql             : SQL query string
        database        : Glue/Athena database name
        output_location : S3 URI where Athena writes results

    Returns:
        Execution ID string, or None on failure.
    """
    try:
        response = client.start_query_execution(
            QueryString=sql,
            QueryExecutionContext={"Database": database},
            ResultConfiguration={"OutputLocation": output_location},
        )
        execution_id = response["QueryExecutionId"]
        logger.info(f"Query submitted. ExecutionId: {execution_id}")
        return execution_id
    except ClientError as e:
        logger.error(f"Failed to start Athena query: {e.response['Error']['Message']}")
        return None


def wait_for_query(
    client,
    execution_id: str,
    poll_interval: float = 1.5,
    max_wait: float = 120.0,
) -> bool:
    """
    Poll Athena until the query finishes (or times out).

    Args:
        execution_id  : ID returned by start_query_execution
        poll_interval : Seconds between status checks
        max_wait      : Maximum total wait time in seconds

    Returns:
        True if SUCCEEDED, False otherwise.
    """
    elapsed = 0.0
    while elapsed < max_wait:
        try:
            resp = client.get_query_execution(QueryExecutionId=execution_id)
            state = resp["QueryExecution"]["Status"]["State"]

            if state == "SUCCEEDED":
                stats = resp["QueryExecution"].get("Statistics", {})
                ms = stats.get("TotalExecutionTimeInMillis", 0)
                logger.info(f"Query SUCCEEDED in {ms}ms. ExecutionId: {execution_id}")
                return True

            elif state in ("FAILED", "CANCELLED"):
                reason = resp["QueryExecution"]["Status"].get("StateChangeReason", "Unknown")
                logger.error(f"Query {state}. Reason: {reason}")
                return False

            elif state in ("QUEUED", "RUNNING"):
                logger.debug(f"Query state: {state}. Waiting {poll_interval}s…")
                time.sleep(poll_interval)
                elapsed += poll_interval
            else:
                logger.warning(f"Unexpected query state: {state}")
                time.sleep(poll_interval)
                elapsed += poll_interval

        except ClientError as e:
            logger.error(f"Error polling query status: {e.response['Error']['Message']}")
            return False

    logger.error(f"Query timed out after {max_wait}s. ExecutionId: {execution_id}")
    return False


def fetch_results(client, execution_id: str, max_rows: int = 10_000) -> pd.DataFrame:
    """
    Fetch Athena query results as a Pandas DataFrame.

    Athena paginates results in pages of 1000 rows.
    This function handles pagination automatically.

    Args:
        execution_id : Completed query execution ID
        max_rows     : Safety cap on total rows to download

    Returns:
        DataFrame with query results (empty on error).
    """
    rows: list[list] = []
    headers: list[str] = []
    next_token: Optional[str] = None
    first_page = True

    try:
        while len(rows) < max_rows:
            kwargs = {
                "QueryExecutionId": execution_id,
                "MaxResults": 1000,
            }
            if next_token:
                kwargs["NextToken"] = next_token

            response = client.get_query_results(**kwargs)
            result_set = response.get("ResultSet", {})

            # First page contains a header row
            data_rows = result_set.get("Rows", [])
            if first_page and data_rows:
                headers = [col["VarCharValue"] for col in data_rows[0]["Data"]]
                data_rows = data_rows[1:]
                first_page = False

            for row in data_rows:
                rows.append([cell.get("VarCharValue", "") for cell in row["Data"]])

            next_token = response.get("NextToken")
            if not next_token:
                break

    except ClientError as e:
        logger.error(f"Failed to fetch Athena results: {e.response['Error']['Message']}")
        return pd.DataFrame()

    if not rows:
        logger.info("Query returned 0 rows.")
        return pd.DataFrame(columns=headers)

    df = pd.DataFrame(rows, columns=headers)
    logger.info(f"Fetched {len(df)} rows, {len(df.columns)} columns.")
    return df


# ---------------------------------------------------------------------------
# High-level convenience function
# ---------------------------------------------------------------------------

def run_query(
    sql: str,
    database: str = ATHENA_DATABASE,
    output_location: str = S3_ATHENA_RESULTS,
    max_rows: int = 10_000,
) -> pd.DataFrame:
    """
    Submit an Athena query and return results as a DataFrame.

    This is the primary function consumers should call.

    Args:
        sql             : SQL query string
        database        : Athena database (default from config)
        output_location : S3 results URI (default from config)
        max_rows        : Row cap for safety

    Returns:
        DataFrame with results, or empty DataFrame on failure.
    """
    client = get_athena_client()

    execution_id = start_query(client, sql, database, output_location)
    if not execution_id:
        return pd.DataFrame()

    success = wait_for_query(client, execution_id)
    if not success:
        return pd.DataFrame()

    return fetch_results(client, execution_id, max_rows=max_rows)


def run_named_query(query_name: str) -> pd.DataFrame:
    """
    Run a pre-built named query from the QUERIES dict.

    Args:
        query_name : Key in QUERIES dict (e.g. 'avg_temperature')

    Returns:
        DataFrame with results.
    """
    if query_name not in QUERIES:
        available = ", ".join(QUERIES.keys())
        raise ValueError(f"Unknown query '{query_name}'. Available: {available}")

    logger.info(f"Running named query: '{query_name}'")
    return run_query(QUERIES[query_name])


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    setup_logging()
    validate_config()

    import argparse
    parser = argparse.ArgumentParser(description="Run an Athena query")
    parser.add_argument(
        "--query",
        type=str,
        default="avg_temperature",
        choices=list(QUERIES.keys()),
        help="Named query to run",
    )
    args = parser.parse_args()

    df = run_named_query(args.query)
    if not df.empty:
        print(f"\nResults for query '{args.query}':")
        print(df.to_string(index=False))
    else:
        print("No results returned.")
