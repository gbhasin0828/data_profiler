"""
config.py — reads and validates config.yaml.

Every database entry has exactly two fields:
  - name            : a label for this connection (used in output)
  - type            : duckdb | sqlite | snowflake | databricks
  - connection_string: how to connect

Connection string formats:
  duckdb      -> path to .duckdb file          e.g.  sample/data.duckdb
  sqlite      -> path to .sqlite file          e.g.  sample/data.sqlite
  snowflake   -> snowflake://user:pass@account/database/schema?warehouse=X&role=Y
  databricks  -> databricks://token:dapi123@host?http_path=/sql/...&catalog=X&schema=Y
"""

from __future__ import annotations
from enum import Enum
from typing import Optional
from pathlib import Path

import yaml
from pydantic import BaseModel, field_validator


class DbType(str, Enum):
    DUCKDB     = "duckdb"
    SQLITE     = "sqlite"
    SNOWFLAKE  = "snowflake"
    DATABRICKS = "databricks"


class StatsDepth(str, Enum):
    BASIC    = "basic"     # row count + schema only
    STANDARD = "standard"  # + min, max, null count, distinct count
    FULL     = "full"      # + histogram, stddev, mean, median


class DatabaseConfig(BaseModel):
    name:              str
    type:              DbType
    connection_string: str


class ProfilerConfig(BaseModel):
    sample_pct:  Optional[float] = None   # None = full scan, 1.0 = 1%
    concurrency: int              = 4     # parallel tables
    stats_depth: StatsDepth       = StatsDepth.STANDARD


class Config(BaseModel):
    output_path: str               = "output/profiles.json"
    profiler:    ProfilerConfig    = ProfilerConfig()
    databases:   list[DatabaseConfig]

    @field_validator("databases")
    @classmethod
    def at_least_one_database(cls, v: list) -> list:
        if not v:
            raise ValueError("at least one database must be specified")
        return v


def load_config(path: str | Path) -> Config:
    """Load and validate config.yaml."""
    with open(path) as f:
        raw = yaml.safe_load(f)
    return Config(**raw)