
# Databricks Medallion Architecture — ETL Data Pipeline

## Project Overview

An end-to-end data engineering pipeline built on **Databricks** using the **Medallion Architecture (Bronze → Silver → Gold)** with PySpark for data transformation. The pipeline ingests historical data from an S3 bucket, applies comprehensive data quality transformations, and produces clean, analytics-ready datasets orchestrated through Databricks Jobs.

---

## Architecture

```
S3 (Landing Zone)
        │
        ▼
┌──────────────────┐
│   BRONZE LAYER   │  ← Raw ingestion (as-is from S3)
└──────────────────┘
        │
        ▼
┌──────────────────┐
│   SILVER LAYER   │  ← Cleaned, validated, transformed
└──────────────────┘
        │
        ▼
┌──────────────────┐
│    GOLD LAYER    │  ← Business-level aggregations & joins
└──────────────────┘
        │
        ▼
   S3 (Processed Folder)
```

---

## Tech Stack

| Technology | Purpose |
|-----------|---------|
| Databricks | Compute & orchestration |
| PySpark | Data transformation |
| AWS S3 | Data storage (Landing & Processed) |
| Delta Lake | Storage format (Bronze/Silver/Gold) |
| Databricks Jobs | Pipeline orchestration |

---

## Data Model

### Dimension Tables
- **Customers** — Customer demographics and details
- **Products** — Product catalog information
- **Price** — Pricing data for products

### Fact Table
- **Orders** — Transactional order data

---

## Transformations Applied

### Data Quality Checks
| # | Transformation | Description |
|---|---------------|-------------|
| 1 | Duplicate Check | Identify and remove duplicate records |
| 2 | Leading/Trailing Spaces | Trim whitespace from string columns |
| 3 | Fat Fingering (Capitalization) | Standardize text casing (upper/lower/title) |
| 4 | Data Type Sync | Ensure correct data types across columns |
| 5 | Unwanted Characters Removal | Remove special characters/symbols using `regexp_replace` |
| 6 | Date Formatting | Standardize multiple date formats, extract year/month |
| 7 | Inner Join | Derive values by joining dimension tables with fact table |

---

## Pipeline Flow

```
1. Historical CSV files land in S3 Landing Zone
2. Databricks reads files from Landing folder
3. Raw data written to Bronze Layer (as-is)
4. Transformations applied → written to Silver Layer
5. Business logic & joins applied → written to Gold Layer
6. Source file moved from Landing → Processed folder
7. Pipeline ready for next incremental file
```

---

## Folder Structure

```
databricks-medallion-pipeline/
│
├── notebooks/
│   ├── 01_bronze_ingestion.py
│   ├── 02_silver_transformations.py
│   ├── 03_gold_aggregations.py
│   └── utils/
│       └── common_functions.py
│
├── data/
│   ├── landing/          # Raw incoming files
│   └── processed/        # Files moved after ingestion
│
├── configs/
│   └── pipeline_config.json
│
├── docs/
│   ├── architecture_diagram.png
│   └── data_model.png
│
├── README.md
└── requirements.txt
```

---

## How to Run

### Prerequisites
- Databricks workspace with cluster configured
- AWS S3 bucket with IAM role access
- PySpark environment

### Steps
1. Clone the repository:
   ```bash
   git clone https://github.com/your-username/databricks-medallion-pipeline.git
   ```
2. Upload notebooks to Databricks workspace
3. Configure S3 paths in `configs/pipeline_config.json`
4. Mount S3 bucket to Databricks:
   ```python
   dbutils.fs.mount(
       source="s3://your-bucket-name",
       mount_point="/mnt/data-pipeline",
       extra_configs={"fs.s3a.access.key": "<KEY>", "fs.s3a.secret.key": "<SECRET>"}
   )
   ```
5. Run notebooks sequentially or trigger the Databricks Job

---

## Databricks Job Orchestration

The pipeline is orchestrated using **Databricks Workflows** with task dependencies:

```
Task 1: Bronze Ingestion
    ↓
Task 2: Silver Transformation
    ↓
Task 3: Gold Aggregation
    ↓
Task 4: Move Files to Processed
```

- **Schedule**: Triggered on new file arrival or scheduled (daily/hourly)
- **Retry Policy**: 2 retries on failure
- **Alerts**: Email notification on success/failure

---

## Key PySpark Code Snippets

### Duplicate Removal
```python
df_deduplicated = df.dropDuplicates()
```

### Trimming Spaces
```python
from pyspark.sql.functions import trim, col
df_trimmed = df.select([trim(col(c)).alias(c) for c in df.columns])
```

### Capitalization Fix
```python
from pyspark.sql.functions import initcap, upper, lower
df = df.withColumn("customer_name", initcap(col("customer_name")))
```

### Regex — Remove Special Characters
```python
from pyspark.sql.functions import regexp_replace
df = df.withColumn("product_name", regexp_replace(col("product_name"), "[^a-zA-Z0-9\\s]", ""))
```

### Date Formatting
```python
from pyspark.sql.functions import to_date, year, date_format
df = df.withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd"))
df = df.withColumn("order_year", year(col("order_date")))
```

---

## Results & Outcomes

- Processed **4 datasets** (3 dimensions + 1 fact table)
- Achieved **100% data quality** post-transformation
- Automated pipeline with **zero manual intervention**
- File management: Landing → Processed ensures no duplicate processing
- Scalable architecture ready for incremental loads

---

## Future Enhancements

- [ ] Implement Auto Loader for streaming ingestion
- [ ] Add data quality framework (Great Expectations)
- [ ] Implement SCD Type 2 for dimension tables
- [ ] Add unit tests for transformations
- [ ] Integrate with a BI tool for Gold layer visualization

---

## Author

**SHOEB KHAN**

---

## License

This project is for educational and portfolio purposes.
