# 📝 Career Assets — AWS Weather Data Pipeline

## Resume Project Description

```
AWS Real-Time Weather Data Pipeline                                  [GitHub Link]
Python · Boto3 · AWS S3 · Glue · Athena · Pandas · Streamlit

• Designed and implemented an end-to-end cloud data pipeline ingesting real-time
  weather data from the OpenWeather REST API for 5 cities using Python and Requests.
• Engineered a data transformation layer with Pandas (schema enforcement, outlier
  detection, derived metrics: heat index, wind chill, comfort level).
• Stored raw JSON (audit layer) and processed Parquet files in AWS S3 using
  Hive-style date/hour partitioning to optimize Athena query performance.
• Automated schema discovery with AWS Glue Crawler, cataloging data in the
  Glue Data Catalog for SQL-queryable access via AWS Athena.
• Built analytical SQL queries in Athena (avg/max temperature, daily trend,
  humidity analysis) querying Parquet files at petabyte scale.
• Developed a multi-section Streamlit dashboard displaying live metrics,
  temperature/humidity charts, and 7-day trend visualizations.
• Applied production practices: environment variable secrets management,
  structured logging, retry logic, unit tests (pytest), .gitignore hygiene.
```

---

## GitHub Repository Description

```
End-to-end AWS data engineering pipeline: OpenWeather API → Python (Pandas) →
S3 (Parquet) → Glue Crawler → Athena SQL → Streamlit dashboard.
Production practices: logging, env vars, retry logic, unit tests, modular code.
```

**Topics/Tags to add on GitHub:**
`aws` `s3` `glue` `athena` `data-engineering` `python` `pandas` `boto3` `streamlit` `etl` `data-pipeline` `openweathermap` `parquet` `portfolio`

---

## 🎤 Interview Questions & Answers

### Architecture & Design

**Q1: Walk me through the architecture of your weather pipeline.**

> "The pipeline has four layers. First, the ingestion layer — `fetch_weather.py` calls the OpenWeather REST API for multiple cities using the `requests` library with retry logic and exponential backoff. Second, the transformation layer — `transform_data.py` uses Pandas to flatten the nested JSON, enforce a schema, validate data quality, and compute derived columns like heat index and wind chill. Third, the storage layer — `upload_to_s3.py` uses Boto3 to write raw JSON (for an audit trail) and processed Parquet files to S3 using Hive-style partitions (`fetch_date=2024-11-01/fetch_hour=12/`) which lets Athena do partition pruning and skip irrelevant files. Fourth, the analytics layer — a Glue Crawler detects the schema from Parquet files into the Glue Data Catalog, and Athena runs standard SQL against that catalog. A Streamlit dashboard ties it together with live charts."

---

**Q2: Why did you choose Parquet over CSV for S3 storage?**

> "Three main reasons. First, Parquet is columnar — Athena only reads the columns you SELECT, not the entire row, which reduces data scanned and therefore cost. Second, Parquet has built-in compression (I used Snappy) which reduces storage cost and network transfer. Third, Parquet preserves data types — integers stay integers, floats stay floats — so you don't need to cast columns in every query the way you would with CSV. For analytical workloads like temperature averages and trend analysis, columnar formats can be 10-100x faster."

---

**Q3: What is Hive-style partitioning and why did you use it?**

> "Hive partitioning stores data in folder paths that encode the partition values, like `fetch_date=2024-11-01/fetch_hour=12/`. When Athena sees a WHERE clause like `WHERE fetch_date = '2024-11-01'`, it only reads the files in that folder and skips all others. This is called partition pruning. Without it, a query for a single day would scan the entire dataset. Glue Crawler also understands Hive paths and automatically adds `fetch_date` and `fetch_hour` as partition columns to the table schema."

---

**Q4: How does AWS Glue fit into the pipeline?**

> "Glue plays two roles. The Glue Crawler scans my S3 Parquet files, infers the schema (column names, types, partitions), and writes that metadata to the Glue Data Catalog — essentially a managed Hive Metastore. Athena then uses the Catalog as its table registry, so when I write `SELECT * FROM weather_db.weather_data`, Athena knows where the files are, what columns exist, and what type each column is. I didn't need to write a CREATE TABLE statement manually."

---

**Q5: How did you handle credentials securely?**

> "I used a `.env` file with `python-dotenv` so credentials are never hardcoded in source code. The `.env` file is listed in `.gitignore` so it's never committed to Git. In production on AWS, I'd replace explicit credentials with IAM Roles — an EC2 instance or Lambda function would assume a role and Boto3 would use the metadata service credential chain automatically, with zero credentials in code or environment variables."

---

### Python & Data Engineering

**Q6: What data quality checks does your transformation layer perform?**

> "Four things. First, schema enforcement — I define the expected dtypes for every column and use `pd.to_numeric(errors='coerce')` to cast them, turning unparseable values to NaN rather than crashing. Second, range validation — I check that temperature is between -90°C and 60°C, humidity is 0-100%, pressure is within realistic atmospheric bounds, and drop rows that violate these. Third, null checks — city name must be present. Fourth, deduplication — I drop exact duplicates on (city, fetch_date, fetch_hour) keeping the latest, which protects against running the pipeline twice in the same hour."

---

**Q7: Why did you use Parquet `BytesIO` instead of writing to disk first?**

> "Writing to disk requires managing temp file paths, cleanup on failure, and won't work in serverless environments like Lambda where `/tmp` is limited. Using `io.BytesIO` I serialize the DataFrame to Parquet in memory and pass the bytes directly to `s3.put_object()`. This is faster, cleaner, and Lambda-compatible."

---

**Q8: How does Athena query execution work programmatically?**

> "Athena is asynchronous — you submit a query and get an execution ID immediately, but the query runs in the background. My `athena_query.py` implements a polling loop: it calls `get_query_execution()` every 1.5 seconds, checking for states QUEUED, RUNNING, SUCCEEDED, FAILED, or CANCELLED. Once SUCCEEDED, it calls `get_query_results()` which paginates 1000 rows at a time using a `NextToken`. I collect all pages, build a list of rows, and return a Pandas DataFrame."

---

**Q9: What retry strategy did you implement and why?**

> "Exponential backoff — I start with a 2 second wait, double it on each retry, and give up after 3 attempts. This matters because the OpenWeather free tier has a rate limit of 60 requests/minute. A naive retry that immediately hammers the API on 429 errors would just get rate-limited again. Doubling the wait time gives the rate limiter time to reset. I also distinguish between retryable errors (429 rate limit, timeouts, connection errors) and non-retryable ones (401 invalid key, 404 city not found) so I don't waste retries on errors that will never succeed."

---

### AWS & Cloud

**Q10: What IAM permissions does the pipeline user need and why?**

> "Minimum required: `s3:PutObject`, `s3:GetObject`, `s3:ListBucket` for reading and writing data; `s3:CreateBucket` if you want auto-creation. For Glue: `glue:GetDatabase`, `glue:GetTable`, `glue:StartCrawler`. For Athena: `athena:StartQueryExecution`, `athena:GetQueryExecution`, `athena:GetQueryResults`. In my project I attached the AWS managed policies `AmazonS3FullAccess`, `AWSGlueServiceRole`, `AmazonAthenaFullAccess` for simplicity, but in a production environment I'd write a custom least-privilege policy document in Terraform."

---

**Q11: If you had to run this pipeline every hour, how would you automate it?**

> "Two options. For a quick solution on an EC2 instance or on-prem server, a cron job calling `python main.py`. For a proper serverless solution: package the pipeline as a Lambda function (the code is already structured for it), use EventBridge (formerly CloudWatch Events) to trigger it on a `rate(1 hour)` schedule, store the API key in AWS Secrets Manager rather than an environment variable, and use a Lambda execution role instead of explicit credentials. This approach has zero infrastructure to manage and costs essentially nothing at this data volume."

---

**Q12: How would you add this to Terraform (Version 2)?**

> "I'd write HCL resources for: `aws_s3_bucket` for the data bucket, `aws_glue_catalog_database` for `weather_db`, `aws_glue_crawler` pointing to the S3 prefix, `aws_iam_role` + `aws_iam_role_policy_attachment` for the Glue service role, and `aws_lambda_function` + `aws_cloudwatch_event_rule` for the scheduled trigger. The benefit is that the entire infrastructure becomes version-controlled, reproducible, and destroyable with `terraform destroy` — critical for cost management in demo projects."

---

### System Design

**Q13: How would you scale this pipeline to 1000 cities?**

> "The current sequential fetch loop would take too long at 1000 cities. I'd parallelize the API calls using `concurrent.futures.ThreadPoolExecutor` — since the work is I/O-bound (network calls), threads are appropriate. I'd set a pool size of ~10-20 to respect the API rate limit. For the storage layer, I'd batch multiple cities into a single Parquet file per partition rather than one per city. For Athena, partitioning by country code in addition to date would improve query performance. At very large scale, I'd switch to AWS Kinesis Data Streams for real-time ingestion and Kinesis Firehose for S3 delivery."

---

**Q14: What's the cost structure of this pipeline?**

> "At small scale it's nearly free. S3 storage is $0.023/GB/month — a Parquet file with 5 cities per hour is under 1MB, so annual storage is negligible. Athena charges $5/TB of data scanned — with Parquet compression and partition pruning, a typical query against this dataset costs a fraction of a cent. Glue Crawler costs $0.44/DPU-hour — a crawl on a small dataset takes a fraction of an hour. The OpenWeather free tier gives 60 calls/minute and 1000 calls/day, which is sufficient for 5 cities at hourly intervals."

---

**Q15: What would you do differently for a production deployment?**

> "Several things: use Terraform for all infrastructure instead of console clicks; store the API key in AWS Secrets Manager; replace explicit credentials with IAM roles; add Great Expectations for formal data quality contracts; implement a dead-letter queue for failed records; add CloudWatch metrics and SNS alerts for pipeline failures; use GitHub Actions for CI/CD to run tests on every pull request; version the Parquet schema to handle future column additions; and add a data lineage layer to track which pipeline run produced each file."

---

## Behavioral Questions

**Q: Tell me about a challenge you faced in this project.**

> "The trickiest part was handling Athena's asynchronous execution model. Unlike a regular database query that blocks until done, Athena returns immediately and you have to poll for results. My first attempt used `time.sleep(5)` — a fixed wait — which was either too slow (simple queries finished in 1 second) or not long enough (complex queries on large datasets). I replaced it with an adaptive polling loop that checks every 1.5 seconds with a 120-second timeout. I also had to handle pagination because Athena returns results in pages of 1000 rows and my first version only got the first page."

**Q: Why did you build this project?**

> "I wanted a project that touched every layer of a real data engineering stack — ingestion, transformation, cloud storage, schema management, and visualization — using technologies that appear on data engineering job descriptions. Building it myself, rather than following a tutorial, forced me to make real architectural decisions: why Parquet over CSV, how to partition for Athena performance, how to handle API rate limits, how to write testable modular code. I also chose a live data source so the pipeline actually runs and produces real results I can show."
