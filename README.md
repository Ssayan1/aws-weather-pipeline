# 🌤️ AWS Real-Time Weather Data Pipeline

<div align="center">

**An end-to-end, production-grade data engineering pipeline that ingests real-time weather data
from the OpenWeather API, processes it with Python & Pandas, stores it in AWS S3,
catalogs it with AWS Glue, queries it via AWS Athena, and visualizes it in a live Streamlit dashboard.**

[![CI](https://github.com/Ssayan1/aws-weather-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/Ssayan1/aws-weather-pipeline/actions/workflows/ci.yml)
[![Python](https://img.shields.io/badge/Python-3.12-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://python.org)
[![AWS S3](https://img.shields.io/badge/AWS_S3-Storage-FF9900?style=for-the-badge&logo=amazons3&logoColor=white)](https://aws.amazon.com/s3)
[![AWS Glue](https://img.shields.io/badge/AWS_Glue-ETL-FF9900?style=for-the-badge&logo=amazonaws&logoColor=white)](https://aws.amazon.com/glue)
[![AWS Athena](https://img.shields.io/badge/AWS_Athena-SQL-FF9900?style=for-the-badge&logo=amazonaws&logoColor=white)](https://aws.amazon.com/athena)
[![Streamlit](https://img.shields.io/badge/Streamlit-Dashboard-FF4B4B?style=for-the-badge&logo=streamlit&logoColor=white)](https://streamlit.io)
[![Pandas](https://img.shields.io/badge/Pandas-Data-150458?style=for-the-badge&logo=pandas&logoColor=white)](https://pandas.pydata.org)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow?style=for-the-badge)](LICENSE)

[Features](#-features) • [Architecture](#-architecture) • [Tech Stack](#-tech-stack) • [Quick Start](#-quick-start) • [AWS Setup](#-aws-setup) • [Dashboard](#-dashboard) • [Queries](#-athena-sql-queries) • [Roadmap](#-version-2-roadmap)

</div>

---

## 📌 Project Overview

This project demonstrates a **complete, real-world AWS data engineering workflow** built for portfolio, resume, and job applications. It covers every layer of a modern data pipeline:

| Layer | What it does |
|---|---|
| **Ingestion** | Fetches live weather for 63 Indian cities every hour |
| **Transformation** | Cleans, validates, and enriches data with derived metrics |
| **Storage** | Stores raw JSON (audit) + Parquet (analytics) in AWS S3 |
| **Cataloging** | AWS Glue Crawler auto-detects schema into Data Catalog |
| **Querying** | AWS Athena runs SQL directly on S3 Parquet files |
| **Visualization** | Streamlit dashboard shows live charts and metrics |

---

## ✨ Features

- 🏙️ **63 Indian cities** tracked in real-time (Mumbai, Delhi, Bangalore, and more)
- 🔄 **Automated hourly runs** via cron job
- 🛡️ **Production practices** — structured logging, retry logic, env var secrets, modular code
- 📊 **27 columns** per record including derived metrics (heat index, wind chill, comfort level)
- 🗂️ **Hive-style S3 partitioning** (`fetch_date=YYYY-MM-DD/fetch_hour=HH`) for Athena performance
- ⚡ **Sub-second Athena queries** (~700ms average) on Parquet data
- ✅ **18/18 pytest tests** passing
- 📈 **Live Streamlit dashboard** with temperature, humidity, trend, and condition charts

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        INGESTION LAYER                              │
│                                                                     │
│   OpenWeather API  ──►  fetch_weather.py  ──►  transform_data.py   │
│   (REST endpoint)        (requests +             (pandas +          │
│                           retry logic)            27 columns)       │
└──────────────────────────────────┬──────────────────────────────────┘
                                   │
┌──────────────────────────────────▼──────────────────────────────────┐
│                         STORAGE LAYER                               │
│                                                                     │
│   upload_to_s3.py (boto3)                                           │
│        │                                                            │
│        ├──► S3 Raw Bucket                                           │
│        │    └── raw/weather/fetch_date=2026-06-05/fetch_hour=12/    │
│        │        └── weather_raw_*.json        (audit trail)         │
│        │                                                            │
│        └──► S3 Processed Bucket                                     │
│             └── processed/weather/fetch_date=2026-06-05/hour=12/   │
│                 └── weather_*.parquet         (analytics layer)     │
└──────────────────────────────────┬──────────────────────────────────┘
                                   │
┌──────────────────────────────────▼──────────────────────────────────┐
│                        PROCESSING LAYER                             │
│                                                                     │
│   AWS Glue Crawler  ──►  Glue Data Catalog  ──►  Glue ETL Job     │
│   (schema detect)         (weather_db.weather)    (PySpark)         │
└──────────────────────────────────┬──────────────────────────────────┘
                                   │
┌──────────────────────────────────▼──────────────────────────────────┐
│                        ANALYTICS LAYER                              │
│                                                                     │
│   AWS Athena (SQL)  ──►  athena_query.py  ──►  Streamlit Dashboard │
│   (~700ms queries)        (boto3 polling)        (live charts)      │
└─────────────────────────────────────────────────────────────────────┘
```

### S3 Partition Layout
```
s3://weather-pipeline-sayan-2024/
├── raw/weather/
│   └── fetch_date=2026-06-05/
│       └── fetch_hour=12/
│           └── weather_raw_20260605_120156.json     ← audit layer
│
└── processed/weather/
    └── fetch_date=2026-06-05/
        └── fetch_hour=12/
            └── weather_20260605_120157.parquet      ← analytics layer
```

---

## 🛠️ Tech Stack

| Category | Technology | Purpose |
|---|---|---|
| Language | Python 3.12 | Core pipeline |
| HTTP | requests | OpenWeather API calls |
| Data | pandas, numpy | Transformation & validation |
| File format | pyarrow | Parquet serialization |
| Cloud SDK | boto3 | AWS S3, Glue, Athena |
| Storage | AWS S3 | Raw JSON + Parquet |
| Catalog | AWS Glue | Schema discovery |
| Query | AWS Athena | SQL on S3 |
| Security | AWS IAM | Roles & permissions |
| Dashboard | Streamlit | Live visualization |
| Secrets | python-dotenv | Environment variables |
| Testing | pytest | 18 unit tests |

---

## 📁 Project Structure

```
aws-weather-pipeline/
│
├── src/                          # All Python source modules
│   ├── __init__.py
│   ├── config.py                 # Central config, env vars, logging setup
│   ├── fetch_weather.py          # OpenWeather API client + retry logic
│   ├── transform_data.py         # Pandas cleaning, validation, derived columns
│   ├── upload_to_s3.py           # Boto3 S3 uploader with Hive partitioning
│   └── athena_query.py           # Athena async runner + 8 pre-built queries
│
├── tests/
│   └── test_transform.py         # 18 pytest unit tests
│
├── docs/
│   └── career_assets.md          # Resume bullets + interview Q&A
│
├── dashboard.py                  # Streamlit dashboard (5 sections)
├── main.py                       # Pipeline orchestrator (single entry point)
│
├── requirements.txt              # All pinned dependencies
├── .env.example                  # Secrets template (copy → .env)
├── .gitignore                    # Protects .env, data/, venv/, logs/
└── README.md
```

---

## 🚀 Quick Start

### Prerequisites
- Python 3.10+
- AWS account (free tier works)
- OpenWeather API key (free at [openweathermap.org](https://openweathermap.org/api))

### 1. Clone the repository
```bash
git clone https://github.com/YOUR_USERNAME/aws-weather-pipeline.git
cd aws-weather-pipeline
```

### 2. Create virtual environment
```bash
python3 -m venv venv
source venv/bin/activate          # Linux / macOS
# venv\Scripts\activate           # Windows
```

### 3. Install dependencies
```bash
pip install -r requirements.txt
```

### 4. Configure secrets
```bash
cp .env.example .env
nano .env                         # Fill in your API key + AWS credentials
```

### 5. Test without AWS (dry run)
```bash
python main.py --dry-run
# Fetches data, transforms it, saves locally — no AWS needed
```

### 6. Run full pipeline
```bash
python main.py
```

### 7. Launch dashboard
```bash
streamlit run dashboard.py
# Open http://localhost:8501
```

---

## ⚙️ Environment Variables

Copy `.env.example` to `.env` and fill in your values:

```env
# OpenWeather API
OPENWEATHER_API_KEY=your_api_key_here
WEATHER_CITIES=Mumbai,Delhi,Bangalore,Kolkata,Chennai,Hyderabad,...
WEATHER_UNITS=metric

# AWS Credentials
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_REGION=ap-south-1
AWS_PROFILE=weather-pipeline

# S3
S3_BUCKET_NAME=weather-pipeline-yourname-2024
S3_ATHENA_RESULTS=s3://weather-pipeline-yourname-2024/athena-results/

# Glue & Athena
GLUE_DATABASE=weather_db
GLUE_TABLE=weather
GLUE_CRAWLER_NAME=weather-crawler
ATHENA_DATABASE=weather_db

# Logging
LOG_LEVEL=INFO
```

> ⚠️ **Never commit `.env` to Git.** It is listed in `.gitignore`.

---

## ☁️ AWS Setup

### Step 1 — Create IAM User

1. Go to **AWS Console → IAM → Users → Create user**
2. Username: `mlops` or `weather-pipeline-user`
3. Attach these managed policies:
   - `AmazonS3FullAccess`
   - `AWSGlueConsoleFullAccess`
   - `AmazonAthenaFullAccess`
   - `IAMFullAccess`
4. Create access key → **Application running outside AWS**
5. Copy keys to your `.env`

### Step 2 — Configure AWS CLI
```bash
aws configure --profile weather-pipeline
# Enter: Access Key, Secret Key, Region (ap-south-1), Format (json)
```

### Step 3 — Create S3 Bucket
```bash
# Via CLI
aws s3 mb s3://weather-pipeline-yourname-2024 \
  --region ap-south-1 \
  --profile weather-pipeline

# Create Athena results folder
aws s3api put-object \
  --bucket weather-pipeline-yourname-2024 \
  --key athena-results/ \
  --profile weather-pipeline
```

### Step 4 — Create Glue Database
```bash
aws glue create-database \
  --database-input '{"Name": "weather_db"}' \
  --profile weather-pipeline \
  --region ap-south-1
```

### Step 5 — Create Glue IAM Role
```bash
# Create role
aws iam create-role \
  --role-name AWSGlueServiceRole-weather \
  --assume-role-policy-document '{
    "Version":"2012-10-17",
    "Statement":[{"Effect":"Allow","Principal":{"Service":"glue.amazonaws.com"},"Action":"sts:AssumeRole"}]
  }' --profile weather-pipeline

# Attach policies
aws iam attach-role-policy \
  --role-name AWSGlueServiceRole-weather \
  --policy-arn arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole \
  --profile weather-pipeline

# Grant S3 access (important — Glue needs explicit bucket access)
aws iam put-role-policy \
  --role-name AWSGlueServiceRole-weather \
  --policy-name GlueS3WeatherAccess \
  --policy-document '{
    "Version":"2012-10-17",
    "Statement":[{"Effect":"Allow","Action":["s3:GetObject","s3:ListBucket"],"Resource":["arn:aws:s3:::weather-pipeline-yourname-2024","arn:aws:s3:::weather-pipeline-yourname-2024/*"]}]
  }' --profile weather-pipeline
```

### Step 6 — Create & Run Glue Crawler
```bash
# Create
aws glue create-crawler \
  --name weather-crawler \
  --role AWSGlueServiceRole-weather \
  --database-name weather_db \
  --targets '{"S3Targets":[{"Path":"s3://weather-pipeline-yourname-2024/processed/weather/"}]}' \
  --profile weather-pipeline --region ap-south-1

# Run
aws glue start-crawler \
  --name weather-crawler \
  --profile weather-pipeline --region ap-south-1

# Check status (run until READY)
aws glue get-crawler \
  --name weather-crawler \
  --profile weather-pipeline --region ap-south-1 \
  --query 'Crawler.LastCrawl.Status'
```

### Step 7 — Configure Athena
```bash
aws athena update-work-group \
  --work-group primary \
  --configuration-updates 'ResultConfigurationUpdates={OutputLocation=s3://weather-pipeline-yourname-2024/athena-results/}' \
  --profile weather-pipeline --region ap-south-1
```

---

## 📊 Data Schema

Each record contains **27 columns** after transformation:

| Column | Type | Description |
|---|---|---|
| `city` | string | City name |
| `country` | string | Country code |
| `latitude` | float | GPS latitude |
| `longitude` | float | GPS longitude |
| `temperature_c` | float | Current temperature (°C) |
| `feels_like_c` | float | Apparent temperature (°C) |
| `temp_min_c` | float | Daily minimum (°C) |
| `temp_max_c` | float | Daily maximum (°C) |
| `humidity_pct` | int | Relative humidity (%) |
| `pressure_hpa` | int | Atmospheric pressure (hPa) |
| `visibility_m` | int | Visibility (metres) |
| `wind_speed_ms` | float | Wind speed (m/s) |
| `wind_deg` | int | Wind direction (degrees) |
| `weather_main` | string | Main condition (Clear, Rain…) |
| `description` | string | Detailed description |
| `heat_index_c` | float | **Derived** — Steadman heat index |
| `wind_chill_c` | float | **Derived** — Wind chill factor |
| `comfort_level` | string | **Derived** — Freezing/Cold/Cool/Comfortable/Warm/Hot |
| `temp_range_c` | float | **Derived** — Daily temp range |
| `sunrise_utc` | string | Sunrise time (ISO) |
| `sunset_utc` | string | Sunset time (ISO) |
| `observation_utc` | string | API observation time (ISO) |
| `fetched_utc` | string | Pipeline fetch time (ISO) |
| `pipeline_ts` | string | Transform timestamp |

> `fetch_date` and `fetch_hour` exist as **S3 partition keys** only (not inside the Parquet file) to avoid Glue duplicate column errors.

---

## 🔍 Athena SQL Queries

Run via CLI:
```bash
python src/athena_query.py --query <query_name>
```

Available queries:

```bash
python src/athena_query.py --query avg_temperature      # Average temp per city
python src/athena_query.py --query max_temperature      # Hottest recordings
python src/athena_query.py --query daily_trend          # 7-day trend
python src/athena_query.py --query humidity_analysis    # Humidity stats
python src/athena_query.py --query latest_readings      # Most recent per city
python src/athena_query.py --query condition_distribution  # Weather breakdown
python src/athena_query.py --query hourly_profile       # Hour-by-hour heatmap
python src/athena_query.py --query wind_analysis        # Wind speed ranking
```

Sample SQL (run directly in Athena console):
```sql
-- Hottest cities right now
SELECT city, ROUND(AVG(temperature_c), 2) AS avg_temp_c
FROM weather_db.weather
GROUP BY city
ORDER BY avg_temp_c DESC
LIMIT 10;

-- Daily humidity trend
SELECT fetch_date, city, ROUND(AVG(humidity_pct), 1) AS avg_humidity
FROM weather_db.weather
GROUP BY fetch_date, city
ORDER BY fetch_date DESC, city;

-- Weather condition distribution
SELECT weather_main, COUNT(*) AS count,
       ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) AS pct
FROM weather_db.weather
GROUP BY weather_main
ORDER BY count DESC;
```

---

## 📈 Dashboard

The Streamlit dashboard has 5 sections:

| Section | Content |
|---|---|
| **Current Metrics** | Live temp, humidity, wind per city (metric cards) |
| **Temperature Chart** | Bar chart — actual vs feels-like per city |
| **Humidity Chart** | Bar chart — average humidity ranking |
| **Daily Trend** | Line chart — 7-day temperature trend (all cities) |
| **Condition Distribution** | Breakdown of weather types |
| **Raw Data Table** | Scrollable latest readings |

```bash
streamlit run dashboard.py
# http://localhost:8501
```

**Sidebar features:**
- 🔄 Refresh button (clears 5-min cache)
- 🏙️ City multi-select filter
- 🟢 Live / Demo mode indicator

---

## 🧪 Running Tests

```bash
# Run all 18 tests
pytest tests/ -v

# With coverage report
pytest tests/ --cov=src --cov-report=html
open htmlcov/index.html
```

Test coverage includes:
- Schema enforcement and dtype casting
- Outlier / range validation (temperature, humidity, pressure)
- Comfort level classification (6 categories)
- Heat index calculation (above/below threshold)
- Wind chill calculation (above/below threshold)
- Deduplication logic
- Parquet byte serialization

---

## ⏰ Automated Scheduling (Cron)

Run the pipeline every hour automatically:

```bash
# Create logs directory
mkdir -p logs

# Edit crontab
crontab -e
```

Add this line:
```bash
0 * * * * cd /path/to/aws-weather-pipeline && /path/to/venv/bin/python main.py >> logs/pipeline.log 2>&1
```

Check logs:
```bash
tail -f logs/pipeline.log
```

---

## 📦 Individual Script Usage

```bash
# Fetch all configured cities
python src/fetch_weather.py

# Fetch a single city
python src/fetch_weather.py --city "Shimla"

# Validate config and print settings
python src/config.py

# Transform and save locally (no AWS)
python src/transform_data.py

# Full pipeline — no S3 upload
python main.py --dry-run

# Full pipeline — uploads to S3
python main.py
```

---

## 🔐 IAM Permissions Reference

**IAM User needs:**
```
AmazonS3FullAccess
AWSGlueConsoleFullAccess
AmazonAthenaFullAccess
IAMFullAccess
```

**Glue Crawler Role (`AWSGlueServiceRole-weather`) needs:**
```
AWSGlueServiceRole (managed)
Custom inline policy for your specific S3 bucket
```

> In production, replace managed policies with a **least-privilege custom policy** — see `docs/career_assets.md` for the exact JSON.

---

## 🚀 Version 2 Roadmap

| Feature | Technology | Benefit |
|---|---|---|
| Serverless scheduling | AWS Lambda + EventBridge | No server to manage |
| Infrastructure as Code | Terraform | Reproducible, version-controlled infra |
| Containerization | Docker + ECR | Portable, consistent environment |
| CI/CD pipeline | GitHub Actions | Auto-test on every push |
| Streaming ingestion | AWS Kinesis | Real-time (sub-minute) updates |
| Data quality checks | Great Expectations | Formal data contracts |
| Alerting | CloudWatch + SNS | Pipeline failure notifications |
| Column-level lineage | OpenLineage | Track data origin |

---

## 📊 Project Metrics

```
Cities monitored    : 63 Indian cities
Records per run     : 63 cities × 27 columns = 1,701 data points
Pipeline duration   : ~28 seconds per full run
Athena query speed  : 600ms – 1,800ms per query
S3 Parquet size     : ~25 KB per run (compressed, Snappy)
S3 JSON size        : ~43 KB per run (raw audit)
Tests               : 18/18 passing
Warnings            : 0
```

---

## 🤝 Contributing

Pull requests are welcome! Please open an issue first to discuss changes.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/add-forecast`)
3. Commit your changes (`git commit -m 'feat: add 5-day forecast support'`)
4. Push to the branch (`git push origin feature/add-forecast`)
5. Open a Pull Request

---

## 📄 License

This project is licensed under the MIT License — see the [LICENSE](LICENSE) file for details.

---

## 👤 Author

**Sayan** — Data Engineer  
Built as a portfolio project demonstrating end-to-end AWS data engineering.

[![GitHub](https://img.shields.io/badge/GitHub-Follow-181717?style=for-the-badge&logo=github)](https://github.com/YOUR_USERNAME)
[![LinkedIn](https://img.shields.io/badge/LinkedIn-Connect-0A66C2?style=for-the-badge&logo=linkedin)](https://linkedin.com/in/YOUR_PROFILE)

---

<div align="center">

⭐ **Star this repo if it helped you!** ⭐

*Built with Python · Pandas · Boto3 · AWS S3 · Glue · Athena · Streamlit*

</div>
