"""
adapters/base.py — abstract interface every database adapter must implement.

The profiler engine only talks to this interface — it never imports a
specific adapter directly. This means adding a new database is just
writing one new class that implements these two methods.
"""

from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class ColumnMeta:
    """Schema metadata for one column — what the column IS."""
    name:             str
    data_type:        str            # raw engine type e.g. "NUMBER(38,0)", "TEXT"
    ordinal_position: int
    is_nullable:      Optional[bool] = None
    comment:          Optional[str]  = None


@dataclass
class ColumnStats:
    """Stats computed for one column — what is IN the column."""
    name:           str
    values_count:   Optional[int]   = None
    null_count:     Optional[int]   = None
    distinct_count: Optional[int]   = None
    distinct_approx: bool           = False   # True if engine used APPROX_COUNT_DISTINCT
    min:            Optional[object]= None
    max:            Optional[object]= None
    mean:           Optional[float] = None
    stddev:         Optional[float] = None
    histogram:      Optional[dict]  = None    # {"boundaries": [...], "frequencies": [...]}


@dataclass
class TableProfile:
    """Everything we know about one table after profiling."""
    database:        str
    schema:          str
    table:           str
    engine:          str
    row_count:       Optional[int]        = None
    sample_pct:      Optional[float]      = None
    columns:         list[ColumnMeta]     = field(default_factory=list)
    column_stats:    list[ColumnStats]    = field(default_factory=list)

    @property
    def fully_qualified_name(self) -> str:
        return f"{self.database}.{self.schema}.{self.table}"


class BaseAdapter(ABC):
    """
    Abstract base class for all database adapters.

    Each adapter is initialized with a connection string and implements:
      - list_tables()  : yields (database, schema, table) tuples
      - get_schema()   : returns column metadata for one table
      - get_stats()    : runs stats SQL and returns ColumnStats per column
    """

    def __init__(self, connection_string: str):
        self.connection_string = connection_string

    @abstractmethod
    def list_tables(self) -> list[tuple[str, str, str]]:
        """
        Return all (database, schema, table) tuples visible to this connection.
        """
        ...

    @abstractmethod
    def get_schema(self, database: str, schema: str, table: str) -> list[ColumnMeta]:
        """
        Return column metadata for one table.
        No stats — just names, types, positions, nullability.
        """
        ...

    @abstractmethod
    def get_stats(
        self,
        database: str,
        schema:   str,
        table:    str,
        columns:  list[ColumnMeta],
        sample_pct: Optional[float],
        stats_depth: str,
    ) -> list[ColumnStats]:
        """
        Run stats queries against one table and return ColumnStats per column.

        Args:
            sample_pct:  None = full scan, 1.0 = 1% sample
            stats_depth: "basic" | "standard" | "full"
        """
        ...

    def profile_table(
        self,
        database:    str,
        schema:      str,
        table:       str,
        sample_pct:  Optional[float],
        stats_depth: str,
    ) -> TableProfile:
        """
        Convenience method — calls get_schema + get_stats and returns
        a complete TableProfile. The engine calls this, not the two
        methods separately.
        """
        columns = self.get_schema(database, schema, table)
        stats   = self.get_stats(database, schema, table, columns, sample_pct, stats_depth)

        return TableProfile(
            database=database,
            schema=schema,
            table=table,
            engine=self.__class__.__name__.replace("Adapter", "").lower(),
            sample_pct=sample_pct,
            columns=columns,
            column_stats=stats,
        )