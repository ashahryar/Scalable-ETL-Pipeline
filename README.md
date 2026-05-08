# YouTube Trending Data Pipeline

A production-grade data pipeline built on AWS that ingests, processes, validates, and serves YouTube trending data using a Medallion Architecture (Bronze → Silver → Gold). The pipeline is fully orchestrated via AWS Step Functions and supports both batch ingestion from static datasets and live data from the YouTube Data API v3.

---

## Architecture Overview

![YouTube Trending Data Pipeline](architecturediagram.png)

The pipeline is organized into six functional layers:

1. **Data Sources** — YouTube Data API v3, Kaggle dataset (CSV + JSON), Amazon EventBridge triggers, external data via Python scripts
2. **Landing Zone (Bronze)** — Raw ingestion via AWS Lambda and Amazon Kinesis Data Streams into Amazon S3 (Bronze bucket)
3. **Processing & Transformation (Silver)** — Schema extraction via AWS Glue Data Catalog, ETL cleaning via AWS Glue Jobs (PySpark), output as partitioned Parquet to Amazon S3 (Silver bucket)
4. **Quality & Validation** — AWS Glue Data Quality checks with a pass/fail gate; failures trigger SNS alerts before any data reaches the Gold layer
5. **Serving Layer (Gold)** — AWS Glue Aggregation jobs produce three business-level tables written to Amazon S3 (Gold bucket) with Glue Data Catalog registration
6. **Analytics & Consumption** — Amazon Athena for SQL queries, Amazon QuickSight for dashboards, Amazon S3 exports

Cross-cutting concerns — IAM, SNS, CloudWatch — apply across all layers.

---

## Tech Stack

| Category | Services |
|---|---|
| Ingestion | AWS Lambda, Amazon Kinesis Data Streams, YouTube Data API v3 |
| Storage | Amazon S3 (Bronze / Silver / Gold buckets), S3 Lifecycle Policies |
| Transformation | AWS Glue Jobs (PySpark), AWS Glue Data Catalog, AWS Glue Crawlers |
| Quality | AWS Glue Data Quality, AWS Lambda (custom checks) |
| Orchestration | AWS Step Functions |
| Notification | Amazon SNS |
| Querying | Amazon Athena |
| Visualization | Amazon QuickSight |
| Monitoring | Amazon CloudWatch Metrics & Logs, CloudWatch Alarms |
| Security | AWS IAM (least privilege), S3 Bucket Policies, AWS KMS Encryption, AWS CloudTrail |

---

## Pipeline Flow

### Orchestration (AWS Step Functions)

```
Trigger (Schedule / EventBridge)
    → Run ETL Jobs (Bronze → Silver)
    → Data Quality Check
        ├── Failed → SNS Failure Alert (pipeline stops)
        └── Passed → Generate Aggregates (Silver → Gold)
                        → Notify Success (SNS)
```

### Data Flow Detail

**Bronze (Raw Ingestion)**
- YouTube Data API v3 responses (JSON) and Kaggle static files (CSV + JSON) land in S3 Bronze via Lambda
- Kinesis Data Streams handles high-throughput ingestion paths
- S3 Lifecycle Policy archives data to Glacier after 90 days
- AWS Glue Crawler infers schema and registers tables in the Bronze Glue database
- Raw data is queryable via Athena for inspection — untouched, no transformations applied

**Silver (Processing & Transformation)**
- Glue Crawler: schema inference, data discovery, catalog update
- Glue ETL Jobs (PySpark): data cleaning, standardization, enrichment, deduplication
- Output: Parquet format, partitioned by region and date, optimized layout
- Glue Data Catalog auto-updated; queryable via Athena immediately post-job

**Quality Gate**
- AWS Glue Data Quality rules run on Silver data
- Checks include: row count threshold, null percentage (<2%), schema validation, value range (e.g., views ≥ 0), data freshness
- On failure: SNS alert sent, pipeline halts — no data advances to Gold
- On pass: pipeline continues to aggregation

**Gold (Serving Layer)**
- Three aggregated tables produced:
  - `trending_analytics` — daily trending summaries per region
  - `channel_analytics` — channel-level performance metrics
  - `category_analytics` — category trends over time
- Written to S3 Gold bucket and registered in Gold Glue catalog
- Consumed via Athena SQL or QuickSight dashboards

---

## Project Structure

```
youtube-trending-pipeline/
├── lambda/
│   ├── ingestion/
│   │   └── lambda_function.py        # YouTube API ingestion
│   ├── json_to_parquet/
│   │   └── lambda_function.py        # JSON → Parquet transformation
│   └── data_quality/
│       └── lambda_function.py        # Data quality checks
├── glue_jobs/
│   ├── bronze_to_silver_statistics.py
│   └── silver_to_gold_analytics.py
├── step_functions/
│   └── pipeline_orchestration.json   # State machine definition
├── scripts/
│   └── aws_copy.sh                   # CLI upload scripts
├── data/                             # Local Kaggle dataset (not committed)
├── architecture_diagram.png
└── README.md
```

---

## Data Sources

**YouTube Data API v3**
- Live trending videos per region (US, GB, IN, CA, etc.)
- Category metadata
- Requires a Google Cloud API key with YouTube Data API v3 enabled

**Kaggle Dataset — YouTube Trending Videos**
- Static multi-region CSV files (video metadata, stats, tags)
- JSON category files per region
- Download from Kaggle and place under `data/`

---

## Prerequisites

- AWS account with appropriate IAM permissions
- AWS CLI installed and configured (`aws configure`)
- Python 3.11
- YouTube Data API v3 key
- Kaggle account (for static dataset download)

---

## Setup & Deployment

### 1. Create S3 Buckets

```bash
# Replace <suffix> with a unique identifier
aws s3 mb s3://yt-data-pipeline-bronze-<suffix>
aws s3 mb s3://yt-data-pipeline-silver-<suffix>
aws s3 mb s3://yt-data-pipeline-gold-<suffix>
aws s3 mb s3://yt-data-pipeline-scripts-<suffix>
```

### 2. Upload Kaggle Static Data

```bash
# Upload CSV statistics
aws s3 cp data/CAvideos.csv s3://yt-data-pipeline-bronze-<suffix>/youtube/raw_statistics/region=ca/

# Upload JSON category files
aws s3 cp data/CA_category_id.json s3://yt-data-pipeline-bronze-<suffix>/youtube/raw_statistics_reference_data/region=ca/

# Repeat for all regions: gb, us, de, fr, in, etc.
```

### 3. Configure IAM Roles

Two roles are required:

**Lambda Execution Role** — attach inline policy granting:
- `s3:GetObject`, `s3:PutObject`, `s3:ListBucket` on Bronze and Silver buckets
- `glue:GetTable`, `glue:CreateTable`, `glue:UpdateTable`, `glue:GetDatabase` on relevant databases
- `sns:Publish` on the alerts topic
- `athena:StartQueryExecution`, `athena:GetQueryExecution`, `athena:GetQueryResults`

**Glue Service Role** — attach `AWSGlueServiceRole` managed policy plus inline policy granting S3 access to all four buckets.

**Step Functions Role** — inline policy granting:
- `lambda:InvokeFunction`
- `glue:StartJobRun`, `glue:GetJobRun`
- `sns:Publish`

### 4. Create SNS Topic & Subscribe

```bash
aws sns create-topic --name yt-data-pipeline-alerts
aws sns subscribe \
  --topic-arn <TOPIC_ARN> \
  --protocol email \
  --notification-endpoint your@email.com
```
Confirm the subscription from your inbox.

### 5. Deploy Lambda Functions

For each function under `lambda/`:

1. Create function in AWS Console (Python 3.11 runtime)
2. Attach the Lambda execution role
3. Add the `AWSSDKPandas-Python311` layer
4. Set environment variables (see below)
5. Set memory to 512 MB, timeout to 5 minutes

**Environment variables — Ingestion Lambda:**

| Key | Value |
|---|---|
| `YOUTUBE_API_KEY` | Your API key |
| `S3_BRONZE_BUCKET` | Bronze bucket name |
| `SNS_ALERT_TOPIC_ARN` | SNS topic ARN |
| `YOUTUBE_REGIONS` | `us,gb,in,ca` |

**Environment variables — JSON to Parquet Lambda:**

| Key | Value |
|---|---|
| `S3_SILVER_BUCKET` | Silver bucket name |
| `GLUE_DB_NAME` | Silver Glue database name |
| `SNS_ALERT_TOPIC_ARN` | SNS topic ARN |

**Environment variables — Data Quality Lambda:**

| Key | Value |
|---|---|
| `S3_SILVER_BUCKET` | Silver bucket name |
| `SNS_ALERT_TOPIC_ARN` | SNS topic ARN |
| `ATHENA_WORK_GROUP` | `primary` |
| `ATHENA_S3_OUTPUT` | `s3://yt-data-pipeline-scripts-<suffix>/athena-results/` |

### 6. Create Glue Databases

```bash
aws glue create-database --database-input '{"Name":"yt-pipeline-bronze"}'
aws glue create-database --database-input '{"Name":"yt-pipeline-silver"}'
aws glue create-database --database-input '{"Name":"yt-pipeline-gold"}'
```

### 7. Run Glue Crawler (Bronze)

Create a crawler in the Glue console pointing to:
- `s3://yt-data-pipeline-bronze-<suffix>/youtube/raw_statistics/`
- `s3://yt-data-pipeline-bronze-<suffix>/youtube/raw_statistics_reference_data/`

Set the target database to `yt-pipeline-bronze-dev`. Run the crawler to register raw tables.

### 8. Deploy Glue ETL Jobs

Upload PySpark scripts to the scripts bucket, then create jobs in Glue console with the following job parameters:

**Bronze → Silver job parameters:**

| Key | Value |
|---|---|
| `--bronze_database` | `yt-pipeline-bronze` |
| `--bronze_table` | `raw_statistics` |
| `--silver_bucket` | Silver bucket name |
| `--silver_database` | `yt-pipeline-silver` |
| `--silver_table` | `clean_statistics` |

**Silver → Gold job parameters:**

| Key | Value |
|---|---|
| `--silver_database` | `yt-pipeline-silver` |
| `--silver_bucket` | Silver bucket name |
| `--gold_bucket` | Gold bucket name |
| `--gold_database` | `yt-pipeline-gold` |

### 9. Deploy Step Functions State Machine

Update `step_functions/pipeline_orchestration.json` with your Lambda ARNs, Glue job names, and SNS ARN. Then create the state machine in the Step Functions console using the Step Functions IAM role.

---

## Running the Pipeline

**Manual execution:**

Trigger the Step Functions state machine from the console or CLI:

```bash
aws stepfunctions start-execution \
  --state-machine-arn <STATE_MACHINE_ARN> \
  --input '{}'
```

**Scheduled execution:**

Create an EventBridge rule with a cron expression targeting the state machine (e.g., every 6 hours):

```
cron(0 */6 * * ? *)
```

---

## Sample Athena Queries

**Top trending days by views (US):**
```sql
SELECT region, trending_date_parsed, total_videos, total_views, avg_views_per_video
FROM trending_analytics
WHERE region = 'us'
ORDER BY total_views DESC
LIMIT 10;
```

**Top channels by region:**
```sql
SELECT channel_title, region, total_views, avg_engagement_rate
FROM channel_analytics
WHERE region = 'in'
ORDER BY total_views DESC
LIMIT 15;
```

**Category performance across regions:**
```sql
SELECT category_name, region, total_videos, avg_views
FROM category_analytics
ORDER BY avg_views DESC;
```

**Cross-region engagement comparison:**
```sql
SELECT region, AVG(avg_engagement_rate) AS avg_engagement, SUM(total_views) AS cumulative_views
FROM trending_analytics
GROUP BY region
ORDER BY avg_engagement DESC;
```

---

## Monitoring & Alerting

- **CloudWatch Logs** — Lambda function logs and Glue job logs
- **CloudWatch Alarms** — Triggered on Lambda error rate or Glue job failures
- **SNS Notifications** — Email alerts for data quality failures and pipeline success/failure at each stage
- **CloudTrail** — Audit log of all API activity across the account

---

## Security & Governance

- **IAM Least Privilege** — each service has a scoped role with only required permissions
- **S3 Bucket Policies** — restrict access to authorized roles only
- **AWS KMS Encryption** — S3 server-side encryption using managed keys
- **AWS CloudTrail** — full audit trail of console and API actions
- **S3 Lifecycle Policies** — raw Bronze data transitions to Glacier after 90 days for cost control

---

## Cost Considerations

All services used have a free tier or pay-per-use pricing. Approximate cost drivers:

- Glue ETL jobs: billed per DPU-hour (2 workers × job duration)
- Lambda: billed per invocation and GB-seconds
- Athena: billed per TB of data scanned (Parquet columnar format minimizes this significantly)
- S3: billed per GB stored; Glacier reduces cost for aged Bronze data
- SNS: negligible for low-volume alerting

For development and testing, total costs should remain within AWS Free Tier limits if the pipeline runs infrequently.

---

## License

MIT
