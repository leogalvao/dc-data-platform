"""SQL view generation utilities."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from semantic.models import SemanticModel

logger = logging.getLogger(__name__)


def generate_view_definitions(
    semantic_model: SemanticModel,
    target_schema: str = "analytics",
) -> dict[str, str]:
    """
    Generate SQL view definitions from a semantic model.

    Args:
        semantic_model: SemanticModel to generate views for
        target_schema: Target schema for views

    Returns:
        Dictionary mapping view names to SQL definitions
    """
    views = {}

    # Generate dimension views
    for dim in semantic_model.dimensions:
        view_name = f"v_dim_{dim.name}"
        sql = f"""
CREATE OR REPLACE VIEW {target_schema}.{view_name} AS
SELECT DISTINCT
    {dim.source_column} AS {dim.name}
FROM {dim.source_table}
WHERE {dim.source_column} IS NOT NULL
ORDER BY {dim.source_column};
"""
        views[view_name] = sql.strip()

    # Generate measure views (aggregated)
    measure_view_name = f"v_{semantic_model.name}_measures"
    measure_cols = []
    for measure in semantic_model.measures:
        measure_cols.append(f"    {measure.expression} AS {measure.name}")

    if measure_cols:
        sql = f"""
CREATE OR REPLACE VIEW {target_schema}.{measure_view_name} AS
SELECT
{',\\n'.join(measure_cols)}
FROM {semantic_model.source_tables[0] if semantic_model.source_tables else 'dual'};
"""
        views[measure_view_name] = sql.strip()

    return views


def generate_materialized_view_refresh_sql(
    view_names: list[str],
    concurrent: bool = True,
) -> str:
    """
    Generate SQL to refresh materialized views.

    Args:
        view_names: List of materialized view names
        concurrent: Whether to use CONCURRENTLY option

    Returns:
        SQL refresh statements
    """
    statements = []
    for view_name in view_names:
        if concurrent:
            statements.append(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {view_name};")
        else:
            statements.append(f"REFRESH MATERIALIZED VIEW {view_name};")
    return "\n".join(statements)


def generate_kpi_views(target_schema: str = "analytics") -> dict[str, str]:
    """
    Generate standard KPI views for the dashboard.

    Returns:
        Dictionary mapping view names to SQL definitions
    """
    views = {}

    # Total spend KPI
    views["v_kpi_total_spend"] = f"""
CREATE OR REPLACE VIEW {target_schema}.v_kpi_total_spend AS
SELECT
    SUM(total_spend) AS total_spend,
    COUNT(DISTINCT vendor_id) AS vendor_count,
    COUNT(*) AS contract_count
FROM {target_schema}.gold_spend_by_vendor;
"""

    # Spend by ward KPI
    views["v_kpi_spend_by_ward"] = f"""
CREATE OR REPLACE VIEW {target_schema}.v_kpi_spend_by_ward AS
SELECT
    ward,
    SUM(total_assessed_value) AS total_value,
    SUM(property_count) AS property_count,
    AVG(avg_assessed_value) AS avg_value
FROM {target_schema}.gold_property_by_ward
GROUP BY ward
ORDER BY ward;
"""

    # Monthly trend KPI
    views["v_kpi_monthly_trend"] = f"""
CREATE OR REPLACE VIEW {target_schema}.v_kpi_monthly_trend AS
SELECT
    date_trunc('month', payment_date) AS month,
    SUM(amount) AS total_spend,
    COUNT(*) AS payment_count
FROM {target_schema}.fact_spend
GROUP BY date_trunc('month', payment_date)
ORDER BY month;
"""

    # CBE spending KPI
    views["v_kpi_cbe_spending"] = f"""
CREATE OR REPLACE VIEW {target_schema}.v_kpi_cbe_spending AS
SELECT
    COALESCE(is_cbe, false) AS is_cbe,
    SUM(total_spend) AS total_spend,
    COUNT(DISTINCT vendor_id) AS vendor_count,
    100.0 * SUM(total_spend) / NULLIF(SUM(SUM(total_spend)) OVER (), 0) AS spend_pct
FROM {target_schema}.gold_spend_by_vendor
GROUP BY COALESCE(is_cbe, false);
"""

    return views


def export_view_sql(views: dict[str, str], output_path: Path) -> None:
    """
    Export view definitions to SQL file.

    Args:
        views: Dictionary of view names to SQL
        output_path: Output file path
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w") as f:
        f.write("-- Generated SQL Views\n")
        f.write(f"-- Generated at: {__import__('datetime').datetime.now().isoformat()}\n\n")

        for view_name, sql in views.items():
            f.write(f"-- {view_name}\n")
            f.write(sql)
            f.write("\n\n")

    logger.info(f"Exported {len(views)} views to {output_path}")
