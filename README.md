# Olist Data Platform

End-to-end lakehouse project on Databricks for the Brazilian E-Commerce (Olist) dataset. Nine raw CSV files are ingested into Bronze, cleaned and validated in Silver, modeled into a dimensional Gold layer, and consumed by a Power BI semantic model and three-page report.

**Stack:** PySpark, Delta Lake, Unity Catalog, Databricks Jobs / SQL Warehouse, Power BI, Git

## Architecture

```text
Kaggle CSVs
    |
Unity Catalog landing volume
    |
Bronze  - raw ingestion + lineage metadata
    |
Silver  - typing, cleaning, deduplication, validation, quarantine
    |
Gold    - 3 facts + 5 conformed dimensions + 1 aggregate
    |
Databricks SQL Warehouse
    |
Power BI Import model (15 DAX measures, 3 report pages, seller-state RLS)
```

## Dataset

Source: **Brazilian E-Commerce Public Dataset by Olist** on Kaggle.

https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce

The project uses the nine related CSV files described in the exercise, representing roughly 100,000 orders from 2016-2018. Source data is intentionally not committed to this repository.

## Key design decisions

- **Bronze** preserves the ingested source values and adds lineage metadata: source file, ingestion timestamp, and batch/run identifier.
- **Silver** centralizes cleaning rules, type enforcement, deduplication, geolocation resolution, category translation, delivery-funnel derivation, and quarantine handling.
- **Gold** keeps order items, payments, and orders in separate fact tables so each measure is evaluated at its correct grain.
- `customer_unique_id` is the customer business key because `customer_id` is order-specific in the source.
- `dim_date` is built in the pipeline rather than in Power BI.
- `agg_daily_category` provides a reporting-oriented pre-aggregation.
- Incremental order processing reads a stored watermark and uses Delta `MERGE` for idempotent upserts.
- Power BI loads Gold only and uses Import mode for fast interactive analysis.

## Gold model

### Facts

| Table | Grain |
|---|---|
| `fact_orders` | One row per order |
| `fact_order_items` | One row per line item |
| `fact_payments` | One row per payment |

### Dimensions

`dim_customer`, `dim_product`, `dim_seller`, `dim_geography`, `dim_date`

### Fan-out handling

Items and payments are intentionally not folded into one fact. A direct order-level join inflates both measures because an order can have multiple items and multiple payment rows.

Validated totals used in the project:

- Item revenue: ~BRL **13.6M** at the correct item grain; ~BRL **14.2M** after fan-out.
- Payment value: ~BRL **16.0M** at the correct payment grain; ~BRL **20.3M** after fan-out.

The Power BI model therefore measures revenue from `fact_order_items` and payment value from `fact_payments` independently.

## How to run from an empty environment

### 1. Clone the repository

```bash
git clone https://github.com/goldenFlori/olist-data-platform.git
cd olist-data-platform
```

### 2. Download the Olist data

Download the nine CSV files from Kaggle and keep the original filenames.

### 3. Bootstrap the Databricks environment

Run:

```text
01-setup/00_bootstrap.sql
```

This creates the `olist` catalog, the required schemas, and the governed landing volume.

### 4. Upload the nine CSV files

Upload the dataset files to:

```text
/Volumes/olist/landing/files/
```

### 5. Run the full pipeline

The orchestrated job is defined in:

```text
resources/jobs/olist_pipeline.yml
```

It runs the layers in dependency order:

```text
Bronze -> Silver -> Gold -> Data Quality
```

You can also run the layer runners directly in this order:

```text
06-orchestration/04_run_bronze
06-orchestration/05_run_silver
06-orchestration/06_run_gold
06-orchestration/03_data_quality
```

### 6. Reproduce the incremental-load simulation

Initialize the control objects:

```text
06-orchestration/01_control_setup
```

Then use:

```text
06-orchestration/02_incremental_demo
```

The history run loads data through the initial cutoff (`2017-12-31`). Incremental mode reads the stored watermark, processes only later orders, MERGEs them into the fact table, and advances the watermark after a successful write.

### 7. Run the backend performance benchmark

Run:

```text
07-performance/01_optimize
```

The current benchmark records the same revenue-by-date query before and after `OPTIMIZE` + Z-ORDER:

```text
Before: 0.84 s
After:  0.63 s
Gain:   25.3%
```

Z-ORDER improves **data skipping** by colocating related values; it is distinct from partition pruning.

### 8. Open Power BI

Start a Databricks SQL Warehouse and open:

```text
powerbi/olist_report.pbix
```

Power BI connects to the Gold layer in Import mode. The current model is about 24 MB and contains 15 DAX measures, a three-page report, and seller-state row-level security.

## Data quality

The pipeline includes both one-time source profiling and an automated DQ gate. The gate checks:

- Non-empty expected tables.
- Freshness based on the latest order date.
- Uniqueness of dimension keys.
- Seven implemented fact-to-dimension referential-integrity relationships.
- Valid ranges for item price, freight value, and review score.

A failed rule stops the pipeline before invalid data reaches Power BI.

Notable source issues handled by the project include multiline review parsing, duplicated reviews, invalid/null review records, inconsistent geolocation city formatting, non-positive payment values, missing product attributes, and a seller city value that had to be recovered from ZIP geography.

## Power BI report

The report contains three pages:

1. **Executive Overview** - overall business performance, headline KPIs, revenue trend, category and state breakdown.
2. **Delivery & Operations** - on-time delivery, funnel timings, delivery variance, state performance, and worst routes.
3. **Product & Seller Performance** - category/seller contribution, customer satisfaction, low-review share, and repeat-customer analysis.

The semantic model uses single-direction dimension-to-fact filtering and marks `dim_date` as the date table.

## Project structure

```text
00-common/         Shared config, helper functions and schemas
01-setup/          Unity Catalog / schema / volume bootstrap
02-bronze/         Raw ingestion, one notebook per source
03-silver/         Profiling, cleaning, validation and quarantine
04-gold/           Facts, dimensions and reporting aggregate
05-analytics/      Reserved for SQL analytics views (planned)
06-orchestration/  Control setup, incremental demo, DQ and layer runners
07-performance/    OPTIMIZE / Z-ORDER benchmark
resources/jobs/    Databricks job definition
powerbi/           Power BI .pbix report
docs/              Project documentation
```

## Environment notes

The project was built on **Databricks Free Edition**. The environment is serverless and has a five-task concurrency limit, so the layers are executed sequentially. Managed tables are used because external storage locations are not available in the selected Free Edition setup.

Power BI Desktop is Windows-only, so the report was developed in a Windows virtual machine and connected to the Databricks SQL Warehouse.

## Documentation

Detailed implementation notes, assumptions, data-quality findings, model design, fan-out explanation, performance results, and challenges are available under `docs/`.
