"""Semantic model definitions."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

logger = logging.getLogger(__name__)


@dataclass
class Dimension:
    """Semantic dimension definition."""

    name: str
    description: str
    source_column: str
    source_table: str
    data_type: str = "string"
    hierarchy: list[str] = field(default_factory=list)

    @classmethod
    def from_dict(cls, name: str, data: dict[str, Any]) -> Dimension:
        return cls(
            name=name,
            description=data.get("description", ""),
            source_column=data.get("source_column", name),
            source_table=data.get("source_table", ""),
            data_type=data.get("data_type", "string"),
            hierarchy=data.get("hierarchy", []),
        )


@dataclass
class Measure:
    """Semantic measure definition."""

    name: str
    description: str
    expression: str
    aggregation: str = "sum"
    format: str = "number"
    unit: str | None = None

    @classmethod
    def from_dict(cls, name: str, data: dict[str, Any]) -> Measure:
        return cls(
            name=name,
            description=data.get("description", ""),
            expression=data.get("expression", ""),
            aggregation=data.get("aggregation", "sum"),
            format=data.get("format", "number"),
            unit=data.get("unit"),
        )


@dataclass
class SemanticModel:
    """Semantic model with dimensions and measures."""

    name: str
    description: str
    source_tables: list[str] = field(default_factory=list)
    dimensions: list[Dimension] = field(default_factory=list)
    measures: list[Measure] = field(default_factory=list)
    relationships: list[dict[str, str]] = field(default_factory=list)

    @classmethod
    def from_yaml(cls, path: Path) -> SemanticModel:
        with open(path) as f:
            data = yaml.safe_load(f)
        return cls.from_dict(data)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> SemanticModel:
        dimensions = [
            Dimension.from_dict(name, dim_data)
            for name, dim_data in data.get("dimensions", {}).items()
        ]
        measures = [
            Measure.from_dict(name, measure_data)
            for name, measure_data in data.get("measures", {}).items()
        ]

        return cls(
            name=data.get("name", ""),
            description=data.get("description", ""),
            source_tables=data.get("source_tables", []),
            dimensions=dimensions,
            measures=measures,
            relationships=data.get("relationships", []),
        )

    def to_dbt_metrics(self) -> dict[str, Any]:
        """Export as dbt metrics YAML."""
        metrics = []
        for measure in self.measures:
            metrics.append({
                "name": measure.name,
                "label": measure.name.replace("_", " ").title(),
                "description": measure.description,
                "type": measure.aggregation,
                "sql": measure.expression,
            })
        return {"metrics": metrics}

    def to_looker_view(self) -> str:
        """Export as LookML view."""
        lines = [f"view: {self.name} {{"]

        for dim in self.dimensions:
            lines.append(f"  dimension: {dim.name} {{")
            lines.append(f'    description: "{dim.description}"')
            lines.append(f"    type: {dim.data_type}")
            lines.append(f"    sql: ${{TABLE}}.{dim.source_column} ;;")
            lines.append("  }")

        for measure in self.measures:
            lines.append(f"  measure: {measure.name} {{")
            lines.append(f'    description: "{measure.description}"')
            lines.append(f"    type: {measure.aggregation}")
            lines.append(f"    sql: {measure.expression} ;;")
            lines.append("  }")

        lines.append("}")
        return "\n".join(lines)
