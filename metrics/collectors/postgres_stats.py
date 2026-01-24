"""Collect metrics from PostgreSQL tables."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

logger = logging.getLogger(__name__)

try:
    import psycopg2
    PSYCOPG2_AVAILABLE = True
except ImportError:
    PSYCOPG2_AVAILABLE = False


@dataclass
class PostgresMetrics:
    """Metrics for a PostgreSQL table."""

    table_name: str
    schema_name: str
    row_count: int
    table_size_bytes: int
    index_size_bytes: int
    column_null_rates: dict[str, float] = field(default_factory=dict)
    last_vacuum: datetime | None = None
    last_analyze: datetime | None = None
    live_tuples: int = 0
    dead_tuples: int = 0

    @property
    def full_name(self) -> str:
        """Full table name with schema."""
        return f"{self.schema_name}.{self.table_name}"

    @property
    def total_size_mb(self) -> float:
        """Total size (table + indexes) in MB."""
        return (self.table_size_bytes + self.index_size_bytes) / (1024 * 1024)

    @property
    def bloat_ratio(self) -> float:
        """Ratio of dead tuples to live tuples."""
        if self.live_tuples == 0:
            return 0.0
        return self.dead_tuples / self.live_tuples

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "table_name": self.table_name,
            "schema_name": self.schema_name,
            "full_name": self.full_name,
            "row_count": self.row_count,
            "table_size_bytes": self.table_size_bytes,
            "index_size_bytes": self.index_size_bytes,
            "total_size_mb": round(self.total_size_mb, 2),
            "column_null_rates": {
                k: round(v, 4) for k, v in self.column_null_rates.items()
            },
            "last_vacuum": self.last_vacuum.isoformat() if self.last_vacuum else None,
            "last_analyze": self.last_analyze.isoformat() if self.last_analyze else None,
            "live_tuples": self.live_tuples,
            "dead_tuples": self.dead_tuples,
            "bloat_ratio": round(self.bloat_ratio, 4),
        }


def collect_postgres_metrics(
    connection_string: str,
    table_name: str,
    schema_name: str = "analytics",
    include_null_rates: bool = True,
) -> PostgresMetrics | None:
    """
    Collect metrics for a PostgreSQL table.

    Args:
        connection_string: PostgreSQL connection string
        table_name: Name of the table
        schema_name: Schema containing the table
        include_null_rates: Whether to calculate null rates (slow for large tables)

    Returns:
        PostgresMetrics or None if not available
    """
    if not PSYCOPG2_AVAILABLE:
        logger.warning("psycopg2 not installed, skipping PostgreSQL metrics")
        return None

    try:
        conn = psycopg2.connect(connection_string)
        cur = conn.cursor()

        # Get row count
        cur.execute(f"SELECT COUNT(*) FROM {schema_name}.{table_name}")
        row_count = cur.fetchone()[0]

        # Get table and index sizes
        cur.execute(
            """
            SELECT
                pg_table_size(c.oid) as table_size,
                pg_indexes_size(c.oid) as index_size
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = %s AND c.relname = %s
            """,
            (schema_name, table_name),
        )
        sizes = cur.fetchone()
        table_size = sizes[0] if sizes else 0
        index_size = sizes[1] if sizes else 0

        # Get vacuum/analyze stats and tuple counts
        cur.execute(
            """
            SELECT
                last_vacuum,
                last_analyze,
                n_live_tup,
                n_dead_tup
            FROM pg_stat_user_tables
            WHERE schemaname = %s AND relname = %s
            """,
            (schema_name, table_name),
        )
        stats = cur.fetchone()
        last_vacuum = stats[0] if stats else None
        last_analyze = stats[1] if stats else None
        live_tuples = stats[2] if stats else 0
        dead_tuples = stats[3] if stats else 0

        # Calculate null rates if requested
        null_rates = {}
        if include_null_rates and row_count > 0:
            # Get column names
            cur.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
                """,
                (schema_name, table_name),
            )
            columns = [row[0] for row in cur.fetchall()]

            # Calculate null rates (sample if table is large)
            sample_clause = ""
            if row_count > 100000:
                sample_clause = "TABLESAMPLE BERNOULLI(1)"

            for col in columns:
                try:
                    cur.execute(
                        f"""
                        SELECT
                            COUNT(*) FILTER (WHERE "{col}" IS NULL)::float / COUNT(*)
                        FROM {schema_name}.{table_name} {sample_clause}
                        """
                    )
                    null_rate = cur.fetchone()[0]
                    null_rates[col] = null_rate or 0.0
                except Exception as e:
                    logger.debug(f"Error calculating null rate for {col}: {e}")

        cur.close()
        conn.close()

        return PostgresMetrics(
            table_name=table_name,
            schema_name=schema_name,
            row_count=row_count,
            table_size_bytes=table_size,
            index_size_bytes=index_size,
            column_null_rates=null_rates,
            last_vacuum=last_vacuum,
            last_analyze=last_analyze,
            live_tuples=live_tuples,
            dead_tuples=dead_tuples,
        )

    except Exception as e:
        logger.error(f"Failed to collect PostgreSQL metrics: {e}")
        return None


def collect_schema_metrics(
    connection_string: str,
    schema_name: str = "analytics",
) -> list[PostgresMetrics]:
    """
    Collect metrics for all tables in a schema.

    Args:
        connection_string: PostgreSQL connection string
        schema_name: Schema to collect metrics for

    Returns:
        List of PostgresMetrics for each table
    """
    if not PSYCOPG2_AVAILABLE:
        logger.warning("psycopg2 not installed")
        return []

    metrics = []

    try:
        conn = psycopg2.connect(connection_string)
        cur = conn.cursor()

        # Get all tables in schema
        cur.execute(
            """
            SELECT tablename
            FROM pg_tables
            WHERE schemaname = %s
            ORDER BY tablename
            """,
            (schema_name,),
        )
        tables = [row[0] for row in cur.fetchall()]

        cur.close()
        conn.close()

        # Collect metrics for each table
        for table_name in tables:
            table_metrics = collect_postgres_metrics(
                connection_string,
                table_name,
                schema_name,
                include_null_rates=False,  # Skip null rates for speed
            )
            if table_metrics:
                metrics.append(table_metrics)

    except Exception as e:
        logger.error(f"Failed to collect schema metrics: {e}")

    return metrics
