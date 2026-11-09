# Databricks Data Engineering: Ingestion Patterns & Lakeflow Jobs

Hands-on Databricks notebooks I built while preparing for Databricks data engineering. They cover the main ways to get data into **Delta tables** on the Databricks Lakehouse:
- batch loads;
- idempotent incremental loads with `COPY INTO`;
- streaming ingestion with **Auto Loader**;
- dynamic, metadata-driven **Lakeflow Jobs**;
- **materialized views** in Databricks SQL.

All data lives in **Unity Catalog** (three-level `catalog.schema.table` names) and **Unity Catalog Volumes** (`/Volumes/...`).

**Stack:** Databricks · Apache Spark / PySpark · Spark SQL · Delta Lake · Auto Loader (`cloudFiles`) · Spark Structured Streaming · Lakeflow Jobs · Unity Catalog · Databricks SQL

> These are learning and practice notebooks built with Databricks sample data (`samples.bakehouse`) and a sample volume. For a full end-to-end project, see [CareSync, a metadata-driven healthcare lakehouse on Microsoft Fabric](https://github.com/d-pavan-de/healthcare). Its gold-layer logic was first written as Databricks notebooks.

---

## What's inside

### 1. Batch ingestion: `Notebooks/databricks learning event/Data Ingestion/Batch Load`
- Creates a Delta table from Parquet with **`read_files()`** (CTAS), capturing file-level lineage from the **`_metadata`** column (`file_name`, `file_modification_time`)
- Does the same in PySpark: `spark.read.format("parquet")` → `saveAsTable`
- Inspects tables with `DESCRIBE TABLE EXTENDED` and `DESCRIBE HISTORY` (Delta transaction log)

### 2. Incremental, idempotent loads: `.../Data Ingestion/Incremental_Load`
- **`COPY INTO`** from Parquet and CSV into Delta tables
- Re-running the load skips files that were already loaded (**exactly-once / idempotent**)
- Column selection during load, header options, and **schema evolution** with `mergeSchema`

### 3. Streaming ingestion with Auto Loader: `.../Data Ingestion/Streaming_*`, `final_autoLoad_csv_to_tbl`

| Notebook | Pattern |
|----------|---------|
| `Streaming_to_table` | Auto Loader (`cloudFiles`) reads Parquet files incrementally from a Volume and writes to a Unity Catalog table |
| `Streaming_volume` | Reads CSV files with **schema inference + schema evolution (`rescue` mode)** and writes Delta files to a Volume path in `append` mode |
| `final_autoLoad_csv_to_tbl` | End-to-end CSV → Delta table stream in a single read/write chain |

All three use **`trigger(availableNow=True)`**, which processes every new file and then stops (incremental batch). They also use a **checkpoint / schema location** for exactly-once file tracking. Results are checked with `DESCRIBE HISTORY`.

### 4. Metadata-driven Lakeflow Jobs: `.../for_lakeflow_jobs`
Notebooks designed as tasks in a **Lakeflow Job** that copies a dynamic list of tables:

```
01_create_lookup_tbl       → writes a lookup (control) table listing the tables to process
02_fetch_tbl_list          → reads the lookup table, publishes the list as a JSON array
                             with dbutils.jobs.taskValues.set("tables_list", ...)
03_cp_tbl_from_sample_...  → parameterized task (widget: table_name), run once per table
                             by a For-each task; copies samples.bakehouse.<table> into
                             dbacademy.bronze as a Delta table
create tbl_insert timestamp → simple task to verify job runs (writes the run timestamp)
```

This is the same **control-table → for-each → parameterized task** pattern used in production ingestion frameworks: adding a table means adding a row, not a new task.

### 5. Reading files: `Notebooks/files`, `sql/files`
- **CSV:** header and schema inference; explicit schemas (DDL string and `StructType`); custom delimiter (`|`), quote, escape and **multiline** records
- **JSON:** single-line and **multiline** JSON
- **SQL on files:** querying files directly (`CSV.`/`PARQUET.` path syntax), `read_files()`, `CREATE OR REPLACE TABLE ... AS SELECT`, and Delta `INSERT` + `DESCRIBE HISTORY`

### 6. Materialized views: `sql/Materialised Views`
- `CREATE OR REPLACE MATERIALIZED VIEW dbacademy.silver.SALES_SUMMARY`: a silver-layer aggregate joining bronze customers and transactions (transactions per country)

## Concepts covered

- Delta Lake tables, table history and metadata
- Batch vs. incremental (`COPY INTO`) vs. streaming (Auto Loader) ingestion, and when to use each
- Exactly-once file processing with checkpoints
- Schema inference and schema evolution (`mergeSchema`, `rescue` mode)
- Unity Catalog namespaces and Volumes
- Lakeflow Jobs: task values, parameterized tasks, for-each over a dynamic list
- Bronze → silver layering with materialized views
- PySpark and Spark SQL side by side

## Repository layout

```
Notebooks/
├── databricks learning event/
│   ├── Data Ingestion/      # batch, COPY INTO, Auto Loader streaming
│   └── for_lakeflow_jobs/   # metadata-driven job tasks
└── files/                   # reading CSV / JSON with PySpark
sql/
├── files/                   # querying Parquet directly
└── Materialised Views/      # Databricks SQL materialized view
```

---

*Author: [Pavan Kumar](https://www.linkedin.com/in/pavan-kumar-darishetti-9888b5364/), Microsoft Certified Fabric Data Engineer Associate, focusing on Databricks and Fabric data engineering.*
