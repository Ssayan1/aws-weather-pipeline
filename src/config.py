"""
config.py
---------
Central configuration module for the AWS Weather Data Pipeline.
Loads all settings from environment variables (.env file).
Never hardcode secrets — use os.environ or python-dotenv.
"""

import os
import logging
from dotenv import load_dotenv

# Load .env file from the project root
load_dotenv()

# Set AWS profile if specified in .env
aws_profile = os.getenv("AWS_PROFILE")
if aws_profile:
    os.environ["AWS_PROFILE"] = aws_profile

# ---------------------------------------------------------------------------
# Logging setup (call once at import time)
# ---------------------------------------------------------------------------

def setup_logging(log_level: str = "INFO") -> logging.Logger:
    """Configure root logger with a consistent format."""
    logging.basicConfig(
        level=getattr(logging, log_level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    return logging.getLogger(__name__)


logger = setup_logging(os.getenv("LOG_LEVEL", "INFO"))


# ---------------------------------------------------------------------------
# OpenWeather API settings
# ---------------------------------------------------------------------------

OPENWEATHER_API_KEY: str = os.getenv("OPENWEATHER_API_KEY", "")
OPENWEATHER_BASE_URL: str = "https://api.openweathermap.org/data/2.5"

# Comma-separated list of cities to fetch  e.g. "London,New York,Tokyo"
CITIES: list[str] = [
    city.strip()
    for city in os.getenv("WEATHER_CITIES", "London,New York,Tokyo,Sydney,Mumbai").split(",")
]

UNITS: str = os.getenv("WEATHER_UNITS", "metric")   # metric | imperial | standard


# ---------------------------------------------------------------------------
# AWS settings
# ---------------------------------------------------------------------------

AWS_REGION: str        = os.getenv("AWS_REGION", "us-east-1")
AWS_ACCESS_KEY_ID: str = os.getenv("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_ACCESS_KEY: str = os.getenv("AWS_SECRET_ACCESS_KEY", "")

# S3
S3_BUCKET_NAME: str    = os.getenv("S3_BUCKET_NAME", "weather-pipeline-raw")
S3_RAW_PREFIX: str     = "raw/weather"
S3_PROCESSED_PREFIX: str = "processed/weather"
S3_ATHENA_RESULTS: str = os.getenv("S3_ATHENA_RESULTS", "s3://weather-pipeline-raw/athena-results/")

# Glue
GLUE_DATABASE: str     = os.getenv("GLUE_DATABASE", "weather_db")
GLUE_TABLE: str = os.getenv("GLUE_TABLE", "weather")
GLUE_CRAWLER_NAME: str = os.getenv("GLUE_CRAWLER_NAME", "weather-crawler")

# Athena
ATHENA_DATABASE: str   = os.getenv("ATHENA_DATABASE", "weather_db")


# ---------------------------------------------------------------------------
# Validation helper
# ---------------------------------------------------------------------------

def validate_config() -> None:
    """
    Validate that all required environment variables are set.
    Raises EnvironmentError with a descriptive message if anything is missing.
    """
    required = {
        "OPENWEATHER_API_KEY": OPENWEATHER_API_KEY,
        "AWS_ACCESS_KEY_ID":   AWS_ACCESS_KEY_ID,
        "AWS_SECRET_ACCESS_KEY": AWS_SECRET_ACCESS_KEY,
        "S3_BUCKET_NAME":      S3_BUCKET_NAME,
    }
    missing = [key for key, val in required.items() if not val]
    if missing:
        raise EnvironmentError(
            f"Missing required environment variables: {', '.join(missing)}\n"
            "Please check your .env file."
        )
    logger.info("Configuration validated successfully.")


if __name__ == "__main__":
    validate_config()
    print(f"Cities to fetch : {CITIES}")
    print(f"S3 bucket       : {S3_BUCKET_NAME}")
    print(f"AWS region      : {AWS_REGION}")
    print(f"Glue database   : {GLUE_DATABASE}")
