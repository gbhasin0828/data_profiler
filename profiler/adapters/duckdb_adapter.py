"""
adapters/duckdb_adapter.py — DuckDB implementation of BaseAdapter.

connection_string: path to a .duckdb file, or ":memory:" for in-memory.

Sampling is pushed into SQL via DuckDB's native BERNOULLI sampling.
Distinct counts are exact — DuckDB is local so cost is acceptable.
Mean/stddev only computed for numeric columns.
Histograms only computed on full depth for numeric columns.
"""

from __future__ import annotations
from typing import Optional

import duckdb

from .base import BaseAdapter, ColumnMeta, ColumnStats

NUMERIC_TYPES = {"INTEGER","BIGINT","DOUBLE","FLOAT","DECIMAL","NUMERIC","HUGEINT","SMALLINT","TINYINT"}


class DuckDBAdapter(BaseAdapter):

    def _connect(self) -> duckdb.DuckDBPyConnection:
        return duckdb.connect(self.connection_string, read_only=True)

    def list_tables(self) -> list[tuple[str, str, str]]:
        con = self._connect()
        rows = con.execute("""
            SELECT table_catalog, table_schema, table_name
            FROM information_schema.tables
            WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
            AND table_type = 'BASE TABLE'
            ORDER BY table_schema, table_name
        """).fetchall()
        con.close()
        return rows

    def get_schema(self, database: str, schema: str, table: str) -> list[ColumnMeta]:
        con = self._connect()
        rows = con.execute("""
            SELECT column_name, data_type, ordinal_position, is_nullable, COLUMN_COMMENT
            FROM information_schema.columns
            WHERE table_schema = ? AND table_name = ?
            ORDER BY ordinal_position
        """, [schema, table]).fetchall()
        con.close()
        return [
            ColumnMeta(name=r[0], data_type=r[1], ordinal_position=r[2],
                       is_nullable=r[3] == "YES", comment=r[4])
            for r in rows
        ]

    def get_stats(self, database: str, schema: str, table: str,
                  columns: list[ColumnMeta], sample_pct: Optional[float],
                  stats_depth: str) -> list[ColumnStats]:

        sample_clause = f"USING SAMPLE {sample_pct} PERCENT (BERNOULLI)" if sample_pct else ""
        fq_table      = f'"{schema}"."{table}"'
        con           = self._connect()
        results       = []

        for col in columns:
            c          = f'"{col.name}"'
            is_numeric = any(t in col.data_type.upper() for t in NUMERIC_TYPES)

            # one query per column — selectively include mean/stddev for numerics
            mean_expr  = f"AVG({c}::DOUBLE)"   if is_numeric else "NULL"
            stddev_expr= f"STDDEV({c}::DOUBLE)" if is_numeric else "NULL"

            row = con.execute(f"""
                SELECT
                    COUNT(*)              AS values_count,
                    COUNT(*) - COUNT({c}) AS null_count,
                    COUNT(DISTINCT {c})   AS distinct_count,
                    MIN({c})              AS min,
                    MAX({c})              AS max,
                    {mean_expr}           AS mean,
                    {stddev_expr}         AS stddev
                FROM {fq_table} {sample_clause}
            """).fetchone()

            histogram = None
            if stats_depth == "full" and is_numeric and row[3] is not None:
                histogram = self._histogram(con, fq_table, c, sample_clause)

            results.append(ColumnStats(
                name=col.name, values_count=row[0], null_count=row[1],
                distinct_count=row[2], min=row[3], max=row[4],
                mean=row[5], stddev=row[6], histogram=histogram,
            ))

        con.close()
        return results

    def _histogram(self, con, fq_table: str, c: str, sample_clause: str) -> Optional[dict]:
        try:
            buckets = con.execute(
                f"SELECT histogram({c}) FROM {fq_table} {sample_clause}"
            ).fetchone()[0]
            if buckets:
                return {"boundaries": list(buckets.keys()), "frequencies": list(buckets.values())}
        except Exception:
            pass
        return None