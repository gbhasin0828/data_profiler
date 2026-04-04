"""
adapters/snowflake_adapter.py — Snowflake implementation of BaseAdapter.

connection_string format:
  snowflake://user:password@account/database/schema?warehouse=WH&role=ROLE

Key differences from DuckDB/SQLite:
  - Uses snowflake-connector-python to connect
  - Sampling via SAMPLE (pct) syntax
  - APPROX_COUNT_DISTINCT for fast cardinality on large tables
  - information_schema scoped to the database in the connection
  - Queries all schemas in the database, not just the one in the URL
"""

from __future__ import annotations
from typing import Optional
from urllib.parse import urlparse, parse_qs

import snowflake.connector

from .base import BaseAdapter, ColumnMeta, ColumnStats

NUMERIC_TYPES = {"NUMBER", "DECIMAL", "NUMERIC", "INT", "INTEGER", "BIGINT",
                 "SMALLINT", "TINYINT", "BYTEINT", "FLOAT", "FLOAT4", "FLOAT8",
                 "DOUBLE", "REAL"}


def _parse_connection_string(cs: str) -> dict:
    """
    Parse snowflake://user:password@account/database/schema?warehouse=WH
    into kwargs for snowflake.connector.connect()

    Uses unquote_plus to handle URL-encoded special characters in password.
    """
    from urllib.parse import unquote_plus
    p = urlparse(cs)
    parts = p.path.strip("/").split("/")
    qs = parse_qs(p.query)
    return {
        "user":      unquote_plus(p.username) if p.username else None,
        "password":  unquote_plus(p.password) if p.password else None,
        "account":   p.hostname,
        "database":  parts[0] if len(parts) > 0 else None,
        "schema":    parts[1] if len(parts) > 1 else None,
        "warehouse": qs.get("warehouse", [None])[0],
        "role":      qs.get("role", [None])[0],
    }


class SnowflakeAdapter(BaseAdapter):

    def _connect(self):
        params = _parse_connection_string(self.connection_string)
        return snowflake.connector.connect(**{k: v for k, v in params.items() if v})

    def list_tables(self) -> list[tuple[str, str, str]]:
        params = _parse_connection_string(self.connection_string)
        database = params["database"]
        con = self._connect()
        cur = con.cursor()
        cur.execute(f"""
            SELECT table_catalog, table_schema, table_name
            FROM {database}.information_schema.tables
            WHERE table_schema NOT IN ('INFORMATION_SCHEMA')
            AND table_type = 'BASE TABLE'
            ORDER BY table_schema, table_name
        """)
        rows = cur.fetchall()
        con.close()
        return rows

    def get_schema(self, database: str, schema: str, table: str) -> list[ColumnMeta]:
        con = self._connect()
        cur = con.cursor()
        cur.execute(f"""
            SELECT column_name, data_type, ordinal_position, is_nullable, comment
            FROM {database}.information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
        """, (schema, table))
        rows = cur.fetchall()
        con.close()
        return [
            ColumnMeta(
                name=r[0], data_type=r[1], ordinal_position=r[2],
                is_nullable=r[3] == "YES", comment=r[4]
            )
            for r in rows
        ]

    def get_stats(self, database: str, schema: str, table: str,
                  columns: list[ColumnMeta], sample_pct: Optional[float],
                  stats_depth: str) -> list[ColumnStats]:

        sample_clause = f"SAMPLE ({sample_pct})" if sample_pct else ""
        fq_table      = f"{database}.{schema}.{table}"
        con           = self._connect()
        cur           = con.cursor()
        results       = []

        for col in columns:
            c          = f'"{col.name}"'
            is_numeric = any(t in col.data_type.upper() for t in NUMERIC_TYPES)

            # APPROX_COUNT_DISTINCT is much faster on large Snowflake tables
            distinct_expr = f"APPROX_COUNT_DISTINCT({c})"
            mean_expr     = f"AVG({c})"    if is_numeric else "NULL"
            stddev_expr   = f"STDDEV({c})" if is_numeric else "NULL"

            cur.execute(f"""
                SELECT
                    COUNT(*)               AS values_count,
                    COUNT(*) - COUNT({c})  AS null_count,
                    {distinct_expr}        AS distinct_count,
                    MIN({c})               AS min,
                    MAX({c})               AS max,
                    {mean_expr}            AS mean,
                    {stddev_expr}          AS stddev
                FROM {fq_table} {sample_clause}
            """)
            row = cur.fetchone()

            results.append(ColumnStats(
                name=col.name,
                values_count=row[0],
                null_count=row[1],
                distinct_count=row[2],
                distinct_approx=True,   # Snowflake always uses APPROX_COUNT_DISTINCT
                min=row[3],
                max=row[4],
                mean=row[5],
                stddev=row[6],
            ))

        con.close()
        return results