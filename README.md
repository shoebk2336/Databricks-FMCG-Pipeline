
# ??️ Databricks Medallion Pipeline — Post-Acquisition Data Integration

## ?? Business Context

A parent company with an established data pipeline and Gold layer analytics **acquired a smaller company** operating in the same product category. The acquired company had **no existing data pipeline** — only raw historical data stored in CSV format.

**Objective:** Build a complete Medallion Architecture pipeline for the acquired company's data, and **upsert the resulting Gold layer into the parent company's existing Gold layer** to create a unified analytics platform for BI reporting.

---

## ?? Problem Statement

```
┌─────────────────────────────────┐      ┌─────────────────────────────────┐
│      PARENT COMPANY             │      │      ACQUIRED COMPANY           │
│                                 │      │                                 │
│  ✅ Full Data Pipeline          │      │  ❌ No Data Pipeline            │
│  ✅ Bronze → Silver → Gold      │      │  ❌ Only raw CSV files in S3    │
│  ✅ BI Dashboards active        │      │  ❌ No data quality checks      │
│  ✅ Gold Layer (production)     │      │  ❌ No analytics layer          │
└─────────────────────────────────┘      └─────────────────────────────────┘
                │                                        │
                │         POST-ACQUISITION               │
                ▼                                        ▼
┌──────────────────────────────────────────────────────────────────┐
│                    UNIFIED GOLD LAYER                             │
│          (Parent + Acquired Company data merged)                  │
│                         ↓                                        │
│                    BI / ANALYTICS                                 │
└──────────────────────────────────────────────────────────────────┘
```

---

## ??️ Architecture

```
ACQUIRED COMPANY DATA PIPELINE (What We Built)
═══════════════════════════════════════════════

    S3 Bucket (Landing Zone)
    ├── customers.csv
    ├── products.csv
    ├── price.csv
    └── orders.csv
            │
            ▼
    ┌──────────────────┐
    │   BRONZE LAYER   │  Raw ingestion (as-is from S3)
    └──────────────────┘
            │
            ▼
    ┌──────────────────┐
    │   SILVER LAYER   │  Cleaned + Transformed (7 transformations)
    └──────────────────┘
            │
            ▼
    ┌──────────────────┐
    │    GOLD LAYER    │  Business-ready (Star Schema)
    └──────────────────┘
            │
            ▼
    ┌──────────────────────────────────────┐
    │   UPSERT INTO PARENT COMPANY GOLD    │  ← MERGE operation
    └──────────────────────────────────────┘
            │
            ▼
    ┌──────────────────┐
    │   UNIFIED BI     │  Combined analytics & reporting
    └──────────────────┘
```

---

## ??️ Tech Stack

| Technology | Purpose |
|-----------|---------|
| **Databricks** | Compute engine & orchestration |
| **PySpark** | Data transformation & processing |
| **AWS S3** | Raw data storage (Landing + Processed) |
| **Delta Lake** | ACID-compliant storage for all layers |
| **Spark SQL** | Gold layer aggregations & MERGE/UPSERT |
| **Databricks Jobs** | Pipeline orchestration & scheduling |

---

## ?? Data Model (Star Schema)

### Dimension Tables
| Table | Description | Key |
|-------|-------------|-----|
| `dim_customers` | Customer demographics & details | customer_id (PK) |
| `dim_products` | Product catalog information | product_id (PK) |
| `dim_price` | Product pricing data | price_id (PK) |

### Fact Table
| Table | Description | Keys |
|-------|-------------|------|
| `fact_orders` | Transactional order records | order_id (PK), customer_id (FK), product_id (FK), price_id (FK) |

---

## ?? Transformations Applied (Silver Layer)

| # | Transformation | Description | PySpark Function |
|---|---------------|-------------|-----------------|
| 1 | **Duplicate Check** | Remove duplicate records | `dropDuplicates()` |
| 2 | **Trim Spaces** | Remove leading/trailing whitespace | `trim()` |
| 3 | **Capitalization Fix** | Standardize text casing (fat fingering) | `initcap()`, `upper()`, `lower()` |
| 4 | **Data Type Sync** | Cast columns to correct types | `cast()`, `to_date()` |
| 5 | **Regex Cleaning** | Remove unwanted characters & symbols | `regexp_replace()` |
| 6 | **Date Formatting** | Uniform date format + extract year/month | `to_date()`, `year()`, `date_format()` |
| 7 | **Inner Join** | Enrich fact table with dimension values | `df.join()` |

---

## ?? Upsert Strategy (Gold → Parent Gold)

The critical step — merging acquired company's Gold layer into the parent company's existing Gold:

```python
# MERGE/UPSERT acquired company Gold into Parent Company Gold
from delta.tables import DeltaTable

parent_gold = DeltaTable.forPath(spark, "/mnt/parent-company/gold/fact_orders")

parent_gold.alias("parent") \
    .merge(
        acquired_gold_df.alias("acquired"),
        "parent.order_id = acquired.order_id"
    ) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()
```

This ensures:
- **New records** from the acquired company are **inserted**
- **Overlapping records** (if any) are **updated** with the latest values
- **No duplicates** in the unified Gold layer

---

## ?? Incremental Load — File Management

```
BEFORE PROCESSING                    AFTER PROCESSING
─────────────────                    ────────────────
s3://bucket/landing/                 s3://bucket/landing/
├── customers.csv                    └── (empty — ready for next file)
├── products.csv
├── price.csv                        s3://bucket/processed/
└── orders.csv                       ├── customers.csv
                                     ├── products.csv
                                     ├── price.csv
                                     └── orders.csv
```

Files are moved from `landing/` → `processed/` after successful Bronze ingestion to prevent reprocessing.

---

## ⚙️ Pipeline Orchestration (Databricks Jobs)

```
┌─────────────────────────────┐
│  Task 1: Bronze Ingestion   │  Read from S3, write raw to Bronze
└──────────────┬──────────────┘
               ▼
┌─────────────────────────────┐
│  Task 2: Silver Transform   │  Apply 7 data quality transformations
└──────────────┬──────────────┘
               ▼
┌─────────────────────────────┐
│  Task 3: Gold Aggregation   │  Star schema joins & business logic
└──────────────┬──────────────┘
               ▼
┌─────────────────────────────┐
│  Task 4: Upsert to Parent   │  MERGE into parent company Gold layer
└──────────────┬──────────────┘
               ▼
┌─────────────────────────────┐
│  Task 5: Move to Processed  │  Archive files from landing folder
└─────────────────────────────┘
```

- **Retry Policy:** 2 retries per task on failure
- **Dependency:** Sequential task execution
- **Alerts:** Email notifications on success/failure

---

## ?? Folder Structure

```
databricks-medallion-pipeline/
│
├── notebooks/
│   ├── 01_bronze_ingestion.py        # S3 → Bronze
│   ├── 02_silver_transformations.py   # Bronze → Silver (7 transforms)
│   ├── 03_gold_aggregation.py         # Silver → Gold (star schema)
│   ├── 04_upsert_to_parent.py         # Gold → Parent Gold (MERGE)
│   └── 05_file_management.py          # Landing → Processed
│
├── configs/
│   └── pipeline_config.json
│
├── docs/
│   ├── architecture_diagram.png
│   ├── star_schema.png
│   └── project_report.pdf
│
├── README.md
└── requirements.txt
```

---

## ?? How to Run

1. **Clone the repo:**
   ```bash
   git clone https://github.com/your-username/databricks-medallion-pipeline.git
   ```

2. **Upload notebooks** to your Databricks workspace

3. **Configure S3 paths** in `configs/pipeline_config.json`:
   ```json
   {
     "landing_path": "s3://acquired-company/landing/",
     "processed_path": "s3://acquired-company/processed/",
     "bronze_path": "/mnt/acquired/bronze/",
     "silver_path": "/mnt/acquired/silver/",
     "gold_path": "/mnt/acquired/gold/",
     "parent_gold_path": "/mnt/parent-company/gold/"
   }
   ```

4. **Create Databricks Job** with the 5 tasks in sequence

5. **Trigger the pipeline** — manually or on schedule

---

## ✅ Results & Outcomes

| Metric | Value |
|--------|-------|
| Datasets processed | 4 (3 dimensions + 1 fact) |
| Transformations applied | 7 data quality checks |
| Pipeline tasks | 5 orchestrated tasks |
| Data integration method | Delta Lake MERGE (upsert) |
| Processing approach | Incremental with file archival |
| Final output | Unified Gold layer for BI |

---

## ?? Future Enhancements

- [ ] Implement Databricks Auto Loader for streaming ingestion
- [ ] Add Delta Live Tables for declarative pipeline management
- [ ] Implement SCD Type 2 for slowly changing dimensions
- [ ] Add Great Expectations data quality framework
- [ ] Build Power BI / Tableau dashboards on unified Gold layer
- [ ] Add data lineage tracking

---

## ?? Author

**SHOEB** — Data Engineer

---

## ?? License

This project is for educational and portfolio purposes.
