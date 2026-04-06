# Data Profiler

A configurable data-profiling utility that scans tables across multiple databases and produces a compact summary of table/column metadata and statistics. Output is compatible with the [OpenMetadata](https://open-metadata.org) schema standard.

---
# Problem to Solve
Databases like DuckDB, SQLite, Postgress & Snowflake stores hundreds of tables with different ways and it becomes challenging to understand what data they contain, how clean it is, or how it compares across systems.
This codebase is a profiler, which answers basic questions about the metadata and data schema. 
The goal of this project is to automate this process — scan any number of tables across any database, compute meaningful statistics per column, and then store the results in a portable format. The output schema is aligned with the OpenMetadata standard so results can be pushed to any live metadata catalogue.

---

# Methodology
I have designed one adapter per database engine, one interface for everything else.
Each database speaks a slightly different SQL commands — e.g., sampling syntax, schema introspection differ slightly between engines so I have created a base file and then created individual files in folder (/adapter) for each Database type. 

Each table in any database goes through the below FOUR steps:

1. Schema discovery — query the database schema to get column names, data types, and comments. This happens through the database itself, not through any external metadata layer.
2. Stats computation — run one SQL query per column to compute null count, distinct count, min, max, mean, and stddev. 
3. Sampling — as tables could be very large, the current program computes stats against a sample rather than the full table. EXAMPLE - Instead of scanning say all 1 billion rows, we tell the database to only look at 1% of them — that's 10 million rows. The stats won't be 100% exact but they'll be close enough to be useful.Most importantly "the database does the work, not Python." If we pull all data into Python then it would become very slow and slugish. 
4. Persistence — results are serialised to JSON using field names from the OpenMetadata tableProfile and columnProfile specification. This means the output is fully compatible with OpenMetadata.

DATABASES COVERED IN THIS PROGRAM
1. DuckDB (Used a file generate_sample_data.py to create a set of sample data)
2. SQLite (Used a file generate_sample_data.py to create a set of sample data)
3. Snowflake (I have used a free tier account and created a sample profile, injected sample data and then used the connection string to pull all data as per the profiler)

---
## Quick Start

```bash
# 1. Clone the repo
git clone https://github.com/gbhasin0828/data_profiler.git
cd data_profiler

# 2. Create and activate a virtual environment
python -m venv venv
venv\Scripts\activate        # Windows
source venv/bin/activate     # Mac/Linux

# 3. Install dependencies
pip install -r requirements.txt

# 4. Generate sample databases (DuckDB + SQLite)
python generate_sample_data.py

# 5. Run the profiler
python -m profiler run --config config.yaml

# Results written to output/profiles.json
```
If you are not going to run the full program, you can see the outputs I have generated in file -  /output/profiles.json
---

## Folder Structure

```
Data_Profiler/
│
├── config.yaml                   # database connections + profiler settings
├── requirements.txt              # pip dependencies
├── generate_sample_data.py       # run once to create sample databases
│
├── sample/
│   ├── data.duckdb               # sample DuckDB database (9 tables, 3 schemas)
│   └── data.sqlite               # sample SQLite database (6 tables)
│
├── output/
│   └── profiles.json             # profiler results written here
│
└── profiler/
    ├── __main__.py               # entry point — python -m profiler run
    ├── config.py                 # reads and validates config.yaml
    ├── engine.py                 # orchestrates all adapters in parallel
    └── adapters/
        ├── base.py               # abstract interface all adapters implement
        ├── duckdb_adapter.py     # DuckDB — local file, native sampling + histogram
        ├── sqlite_adapter.py     # SQLite — local file, no native sampling
        └── snowflake_adapter.py  # Snowflake — APPROX_COUNT_DISTINCT, SAMPLE clause
```

---

## Configuration

All settings live in `config.yaml`:

```yaml
output_path: output/profiles.json

profiler:
  sample_pct: 1.0       # scan 1% of rows — remove for full scan
  concurrency: 4        # number of tables to profile in parallel
  stats_depth: standard # basic | standard | full

databases:
  - name: local_duckdb
    type: duckdb
    connection_string: sample/data.duckdb

  - name: local_sqlite
    type: sqlite
    connection_string: sample/data.sqlite

  - name: snowflake_prod
    type: snowflake
    connection_string: snowflake://user:password@account.region/DATABASE/SCHEMA?warehouse=COMPUTE_WH&role=SYSADMIN
```

### Stats depth

| Depth      | What is computed                                              |
|------------|---------------------------------------------------------------|
| `basic`    | row count, null count, distinct count                         |
| `standard` | + min, max, mean                                              |
| `full`     | + stddev, histogram (numeric columns only)                    |

### Sampling

Set `sample_pct` to scan a percentage of rows instead of the full table. Each engine uses its native sampling mechanism:

| Engine     | Sampling method                                      |
|------------|------------------------------------------------------|
| DuckDB     | `USING SAMPLE {pct} PERCENT (BERNOULLI)`             |
| SQLite     | `WHERE ABS(RANDOM()) % 100 < {pct}` (approximate)   |
| Snowflake  | `SAMPLE ({pct})`                                     |

Remove `sample_pct` from config (or set to `null`) for a full table scan.

---

## Output

Results are written as JSON to `output/profiles.json`. Each table entry looks like:

```json
{
  "database": "data",
  "schema": "sales",
  "table": "orders",
  "engine": "duckdb",
  "row_count": 20000,
  "sample_pct": 1.0,
  "columns": [
    { "name": "order_id", "data_type": "INTEGER", "ordinal_position": 1, "is_nullable": false }
  ],
  "column_stats": [
    {
      "name": "order_id",
      "values_count": 200,
      "null_count": 0,
      "distinct_count": 200,
      "min": 1,
      "max": 20000,
      "mean": 10000.5,
      "stddev": 5773.6,
      "histogram": {
        "boundaries": [1, 2001, 4001, 6001, 8001],
        "frequencies": [40, 40, 40, 40, 40]
      }
    }
  ]
}
```

The output schema aligns with the [OpenMetadata `tableProfile` and `columnProfile` spec](https://github.com/open-metadata/OpenMetadata/blob/main/openmetadata-spec/src/main/resources/json/schema/entity/data/table.json) — field names are identical so results can be pushed directly to a live OpenMetadata server via `om_client.py`.


---

## Required Privileges

| Database   | Required privileges                                      |
|------------|----------------------------------------------------------|
| DuckDB     | Read access to the `.duckdb` file                        |
| SQLite     | Read access to the `.sqlite` file                        |
| Snowflake  | `USAGE` on warehouse, database, schema. `SELECT` on tables. `REFERENCES` on `information_schema`. |

---
