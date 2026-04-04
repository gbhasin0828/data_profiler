"""
engine.py — orchestrates profiling across all configured databases.

For each database in config:
  1. Pick the right adapter
  2. List all tables
  3. Profile each table in parallel
  4. Write results to output JSON
"""

from __future__ import annotations
import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict
from datetime import datetime

from .config import Config, DbType
from .adapters.duckdb_adapter import DuckDBAdapter
from .adapters.sqlite_adapter import SQLiteAdapter


def _get_adapter(db):
    if db.type == DbType.DUCKDB:
        return DuckDBAdapter(db.connection_string)
    if db.type == DbType.SQLITE:
        return SQLiteAdapter(db.connection_string)
    if db.type == DbType.SNOWFLAKE:
        from .adapters.snowflake_adapter import SnowflakeAdapter
        return SnowflakeAdapter(db.connection_string)
    if db.type == DbType.DATABRICKS:
        from .adapters.databricks_adapter import DatabricksAdapter
        return DatabricksAdapter(db.connection_string)
    raise ValueError(f"Unknown database type: {db.type}")


def run(config: Config) -> dict:
    results = {"profiled_at": datetime.utcnow().isoformat(), "tables": []}

    for db in config.databases:
        print(f"\n[{db.name}] connecting...")
        adapter = _get_adapter(db)
        tables  = adapter.list_tables()
        print(f"[{db.name}] found {len(tables)} tables — profiling with {config.profiler.concurrency} workers...")

        def profile_one(t):
            database, schema, table = t
            print(f"  profiling {database}.{schema}.{table}")
            return asdict(adapter.profile_table(
                database, schema, table,
                config.profiler.sample_pct,
                config.profiler.stats_depth.value,
            ))

        with ThreadPoolExecutor(max_workers=config.profiler.concurrency) as pool:
            futures = {pool.submit(profile_one, t): t for t in tables}
            for future in as_completed(futures):
                try:
                    results["tables"].append(future.result())
                except Exception as e:
                    t = futures[future]
                    print(f"  ERROR profiling {t}: {e}")

    os.makedirs(os.path.dirname(config.output_path) or ".", exist_ok=True)
    with open(config.output_path, "w") as f:
        json.dump(results, f, indent=2, default=str)

    print(f"\nDone. Results written to {config.output_path}")
    return results