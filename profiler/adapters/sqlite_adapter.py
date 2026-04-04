"""
adapters/sqlite_adapter.py — SQLite implementation of BaseAdapter.

connection_string: path to a .sqlite file.
  e.g.  sample/data.sqlite

Key differences from DuckDB:
  - Schema via PRAGMA table_info() not information_schema
  - Table list via sqlite_master not information_schema.tables
  - No native sampling — faked with WHERE ABS(RANDOM()) % 100 < {pct}
  - No APPROX_COUNT_DISTINCT — exact COUNT(DISTINCT) only
  - No histogram function — skipped even on full depth
  - No stddev built-in — skipped for non-numeric columns
"""

from __future__ import annotations
from typing import Optional
import sqlite3

from .base import BaseAdapter, ColumnMeta, ColumnStats

NUMERIC_TYPES = {"INTEGER", "REAL", "NUMERIC", "FLOAT", "DOUBLE", "DECIMAL", "INT", "BIGINT"}


class SQLiteAdapter(BaseAdapter):

    def _connect(self) -> sqlite3.Connection:
        con = sqlite3.connect(self.connection_string)
        con.row_factory = sqlite3.Row
        return con

    def list_tables(self) -> list[tuple[str, str, str]]:
        con = self._connect()
        rows = con.execute("""
            SELECT name FROM sqlite_master
            WHERE type = 'table'
            AND name NOT LIKE 'sqlite_%'
            ORDER BY name
        """).fetchall()
        con.close()
        # SQLite has no database/schema concept — use filename as database, 'main' as schema
        db_name = self.connection_string.split("/")[-1].replace(".sqlite", "")
        return [(db_name, "main", row["name"]) for row in rows]

    def get_schema(self, database: str, schema: str, table: str) -> list[ColumnMeta]:
        con = self._connect()
        # PRAGMA table_info returns: (cid, name, type, notnull, dflt_value, pk)
        rows = con.execute(f"PRAGMA table_info('{table}')").fetchall()
        con.close()
        return [
            ColumnMeta(
                name=row["name"],
                data_type=row["type"] or "TEXT",
                ordinal_position=row["cid"] + 1,
                is_nullable=not row["notnull"],
            )
            for row in rows
        ]

    def get_stats(self, database: str, schema: str, table: str,
                  columns: list[ColumnMeta], sample_pct: Optional[float],
                  stats_depth: str) -> list[ColumnStats]:

        # SQLite has no native sampling — approximate with RANDOM()
        # ABS(RANDOM()) % 100 < pct gives roughly pct% of rows
        sample_clause = (
            f"WHERE ABS(RANDOM()) % 100 < {int(sample_pct)}"
            if sample_pct else ""
        )

        con = self._connect()
        results = []

        for col in columns:
            c          = f'"{col.name}"'
            is_numeric = any(t in col.data_type.upper() for t in NUMERIC_TYPES)
            mean_expr  = f"AVG({c})"    if is_numeric else "NULL"
            stddev_expr= f"AVG(({c} - (SELECT AVG({c}) FROM \"{table}\" {sample_clause})) * ({c} - (SELECT AVG({c}) FROM \"{table}\" {sample_clause})))" if is_numeric else "NULL"

            row = con.execute(f"""
                SELECT
                    COUNT(*)               AS values_count,
                    COUNT(*) - COUNT({c})  AS null_count,
                    COUNT(DISTINCT {c})    AS distinct_count,
                    MIN({c})               AS min,
                    MAX({c})               AS max,
                    {mean_expr}            AS mean
                FROM "{table}" {sample_clause}
            """).fetchone()

            results.append(ColumnStats(
                name=col.name,
                values_count=row["values_count"],
                null_count=row["null_count"],
                distinct_count=row["distinct_count"],
                min=row["min"],
                max=row["max"],
                mean=row["mean"],
            ))

        con.close()
        return results