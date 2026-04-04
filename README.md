# Data Profiler

A configurable data-profiling utility that scans tables across multiple databases and produces a compact summary of table/column metadata and statistics. Output is compatible with the [OpenMetadata](https://open-metadata.org) schema standard.

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

## Assumptions

- Python 3.11 or higher
- For Snowflake: `snowflake-connector-python >= 3.14.0` (supports Python 3.14)
- Snowflake connection strings must URL-encode special characters in passwords
- SQLite sampling is approximate — `RANDOM()` distribution is not perfectly uniform
- `output/` directory is created automatically if it doesn't exist