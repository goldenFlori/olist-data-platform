# Olist Data Platform

End-to-end lakehouse project on Databricks for the Brazilian E-Commerce (Olist) dataset. Nine raw CSV files are ingested into Bronze with Auto Loader, cleaned and validated in Silver, modeled into a dimensional Gold layer, orchestrated as a Databricks job, and consumed by Power BI.

**Stack:** PySpark, Delta Lake, Unity Catalog, Databricks Jobs, Databricks SQL Warehouse, Power BI, GitHub Actions, PySpark ML

**Repository:** https://github.com/goldenFlori/olist-data-platform

## Architecture

```text
Kaggle CSVs (9 files)
        |
Unity Catalog landing volume
        |
Bronze - Auto Loader / Structured Streaming + lineage metadata + Delta
        |
Silver - profiling, typing, cleaning, deduplication, validation, quarantine
        |
Gold - 3 facts + 5 dimensions + 1 aggregate
        |
Databricks SQL Warehouse
        |
Power BI Import report

Bonus paths:
Silver -> Random Forest -> gold.order_delivery_predictions -> Power BI
Gold -> Unity Catalog row filters -> DirectQuery RLS demonstration
GitHub push -> GitHub Actions -> databricks bundle validate
```

## Dataset

Source: **Brazilian E-Commerce Public Dataset by Olist** on Kaggle.

https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce

The project uses the nine related CSV files from the exercise, representing roughly 100,000 orders from 2016-2018. Source data is not committed to the repository.

Final Bronze row counts after Auto Loader ingestion:

| Source | Rows |
|---|---:|
| orders | 99,441 |
| order_items | 112,650 |
| order_payments | 103,886 |
| order_reviews | 99,224 |
| customers | 99,441 |
| sellers | 3,095 |
| products | 32,951 |
| geolocation | 1,000,163 |
| category_tr | 71 |

All nine Bronze tables were validated with non-null ingestion metadata and independent streaming checkpoints.

## Key design decisions

- **Bronze** preserves source values and adds `source_file`, `ingestion_timestamp`, and `batch_id`.
- Bronze ingestion uses Databricks **Auto Loader (`cloudFiles`)** with explicit schemas, `availableNow=True`, and one checkpoint per source.
- `order_reviews` is read with multiline support; `geolocation` is partitioned by `geolocation_state`.
- **Silver** centralizes type enforcement, profiling, deduplication, category translation, geolocation resolution, delivery-funnel derivation, and quarantine rules.
- **Gold** keeps orders, order items, and payments in separate facts so every metric is evaluated at its correct grain.
- `customer_unique_id` is the customer business key because source `customer_id` is order-specific.
- `dim_date` is created in the pipeline, not in Power BI.
- `agg_daily_category` provides a reporting-oriented pre-aggregation.
- Incremental order processing reads a stored watermark and uses Delta `MERGE` for idempotent upserts.
- Power BI loads Gold only; the primary report uses Import mode.
- Lakehouse row-level security is enforced with Unity Catalog row filters for seller-state access.
- A Random Forest regression model writes delivery-delay predictions to a separate Gold table instead of modifying the core fact model.

## Gold model

### Core facts

| Table | Grain |
|---|---|
| `fact_orders` | One row per order |
| `fact_order_items` | One row per line item |
| `fact_payments` | One row per payment |

### Dimensions

`dim_customer`, `dim_product`, `dim_seller`, `dim_geography`, `dim_date`

### Reporting / bonus outputs

- `agg_daily_category` - daily category reporting aggregate.
- `order_delivery_predictions` - Bonus 5 ML output with one row per delivered order used for model scoring/reporting.

### Fan-out handling

Items and payments are intentionally not folded into one fact. An order can have multiple items and multiple payment rows, so a direct join by `order_id` duplicates both sides.

Validated totals used in the project:

- Correct item revenue: approximately **BRL 13.6M**.
- Item revenue after a fan-out join: approximately **BRL 14.2M**.
- Correct payment value: approximately **BRL 16.0M**.
- Payment value after a fan-out join: approximately **BRL 20.3M**.

The Power BI model therefore measures revenue from `fact_order_items` and payment value from `fact_payments` independently.

## How to run from an empty environment

### 1. Clone the repository

```bash
git clone https://github.com/goldenFlori/olist-data-platform.git
cd olist-data-platform
```

### 2. Download the Olist data

Download the nine CSV files from Kaggle and keep the original filenames.

### 3. Bootstrap Databricks

Run:

```text
01-setup/00_bootstrap.sql
```

This creates the `olist` catalog, required schemas, and governed landing volume.

### 4. Upload the nine source files

Upload the CSV files to:

```text
/Volumes/olist/landing/files/
```

### 5. Run the main pipeline

The job definition is:

```text
resources/jobs/olist_pipeline.yml
```

Its dependency chain is:

```text
run_bronze -> run_silver -> run_gold -> data_quality
```

The layer runners are:

```text
06-orchestration/04_run_bronze.ipynb
06-orchestration/05_run_silver.ipynb
06-orchestration/06_run_gold.ipynb
06-orchestration/03_data_quality.ipynb
```

The current DQ run passes the implemented row-count/freshness, uniqueness, seven referential-integrity relationships, and range checks.

### 6. Reproduce the incremental-load simulation

Initialize the control objects:

```text
06-orchestration/01_control_setup
```

Then run:

```text
06-orchestration/02_incremental_demo
```

The historical load uses `2017-12-31` as the initial cutoff. Later runs read the stored watermark, process only newer orders, `MERGE` into the target, and advance the watermark after success.

### 7. Run the backend performance benchmark

Run:

```text
07-performance/01_optimize
```

Measured revenue-by-date query:

```text
Before: 0.84 s
After:  0.63 s
Gain:   25.3%
```

Z-ORDER improves **data skipping** by colocating related values. It is not the same mechanism as partition pruning.

### 8. Validate the Databricks Asset Bundle

The bundle root is:

```text
databricks.yml
```

and includes:

```text
resources/jobs/*.yml
```

Local validation:

```bash
databricks bundle validate --target dev
```

GitHub Actions runs the same validation from:

```text
.github/workflows/validate-bundle.yml
```

The workflow authenticates through repository secrets `DATABRICKS_HOST` and `DATABRICKS_TOKEN`. No token is stored in source control. The CI step validates the bundle configuration; it does not deploy or execute the pipeline.

### 9. Run the optional lakehouse RLS bonus

Run:

```text
04-gold/10_rls_lakehouse.ipynb
```

The notebook creates an entitlement mapping and Unity Catalog row-filter functions, then applies row filters to seller-scoped Gold tables. The validation used seller state `SP` and confirmed zero visible rows outside the assigned state.

### 10. Run the ML delivery-delay bonus

Run:

```text
04-gold/11_delivery_delay_predictions.ipynb
```

The notebook trains a PySpark ML Random Forest regression model from Silver data and writes:

```text
olist.gold.order_delivery_predictions
```

Final test-set results:

- Chronological split: 80% train / 20% test.
- Train rows: **77,172**.
- Test rows: **19,298**.
- Test MAE from Gold: approximately **5.05 days**.
- Median baseline MAE: **7.57 days**.
- Test R2: **0.530**.
- MAE improvement versus baseline: approximately **33%**.

### 11. Open Power BI

Primary Import report:

```text
powerbi/olist_report_import.pbix
```

DirectQuery variant:

```text
powerbi/olist_report_directquery.pbix
```

The Import report contains the three required business pages plus a fourth ML validation page:

1. Executive Overview
2. Delivery & Operations
3. Product & Seller Performance
4. Delivery Delay Prediction

The base semantic model contains 15 core DAX measures; the ML page adds prediction-focused measures.

## Bonus features

| Bonus | Status | Implementation |
|---|---|---|
| **1 - Auto Loader / Structured Streaming** | Complete | All nine Bronze sources use `cloudFiles`, source-specific checkpoints, explicit schemas, and `availableNow` processing. |
| **2 - Databricks Asset Bundle + CI** | Complete | `databricks.yml`, `resources/jobs/olist_pipeline.yml`, and GitHub Actions `databricks bundle validate --target dev`. |
| **3 - Lakehouse RLS** | Complete | Unity Catalog entitlement mapping + row-filter UDFs on `dim_seller` and `fact_order_items`; SP validation passed. |
| **4 - Import + DirectQuery** | Partial relative to the exercise wording | Import and DirectQuery Power BI versions were created, and DirectQuery is used to demonstrate lakehouse RLS. A formal refresh-time, query-time, and model-size comparison was not performed. |
| **5 - ML model output in Gold** | Complete | Random Forest delivery-delay predictions are written to `gold.order_delivery_predictions` and shown alongside actual delay in Power BI. |

## Data quality

The pipeline includes one-time source profiling plus an automated DQ gate. The gate validates:

- Expected non-empty tables and source freshness.
- Dimension-key uniqueness.
- Seven implemented fact-to-dimension referential-integrity checks.
- Valid ranges for item price, freight value, and review score.

Notable issues handled include multiline review parsing, duplicate/invalid reviews, geolocation normalization, non-positive payment values, missing product attributes, and repair of a malformed seller city using ZIP-based geography.

## Power BI model and reporting

The primary Power BI model uses single-direction relationships and marks `dim_date` as the date table. Revenue, payment value, order-level delivery metrics, and customer metrics remain on their natural fact grains.

The original three-page Import model was approximately **24 MB** before the bonus ML output was added. No new formal Import-versus-DirectQuery performance comparison is claimed after the bonus work.

The DirectQuery variant is used primarily to demonstrate source-backed query behavior and Unity Catalog RLS. Because the lakehouse RLS demonstration restricts seller-scoped data to `SP`, a raw performance comparison against the full Import model would not be a strict like-for-like benchmark.

## Project structure

```text
.github/workflows/   GitHub Actions bundle validation
00-common/           Shared config, schemas, Auto Loader helper
01-setup/            Unity Catalog / schema / volume bootstrap
02-bronze/           Auto Loader ingestion, one notebook per source
03-silver/           Profiling, cleaning, validation, quarantine
04-gold/             Facts, dimensions, aggregate, RLS and ML bonus notebooks
05-analytics/        Analytics work
06-orchestration/    Control setup, incremental demo, DQ and layer runners
07-performance/      OPTIMIZE / Z-ORDER benchmark
resources/jobs/      Databricks job definition
powerbi/             Import / DirectQuery PBIX reports and screenshots
docs/                Project documentation
databricks.yml       Databricks Asset Bundle root configuration
```

## Environment notes

The project was built in a Databricks Free Edition / serverless environment. The layer runners execute sequentially, which also keeps the dependency path explicit and reproducible.

Power BI Desktop is Windows-only, so report development was performed in a Windows environment connected to the Databricks SQL Warehouse.

## Documentation

Detailed architecture, data model, assumptions, data-quality findings, incremental design, performance results, bonus implementations, ML methodology, and challenges are documented under `docs/`.
