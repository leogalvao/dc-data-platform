"""Pydantic settings for DC Data Platform configuration."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Literal

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


def get_project_root() -> Path:
    """Get the project root directory."""
    return Path(__file__).parent.parent


class Settings(BaseSettings):
    """DC Data Platform configuration settings."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="DC_",
        extra="ignore",
    )

    # Environment
    environment: Literal["local", "dev", "prod"] = Field(
        default="local",
        description="Deployment environment",
    )

    # Project root (auto-detected)
    project_root: Path = Field(
        default_factory=get_project_root,
        description="Path to project root",
    )

    # Database settings
    warehouse_db_host: str = Field(
        default="localhost",
        description="PostgreSQL host for warehouse",
    )
    warehouse_db_port: int = Field(
        default=5432,
        description="PostgreSQL port for warehouse",
    )
    warehouse_db_name: str = Field(
        default="dc_analytics",
        description="PostgreSQL database name",
    )
    warehouse_db_user: str = Field(
        default="postgres",
        description="PostgreSQL user",
    )
    warehouse_db_password: str = Field(
        default="",
        description="PostgreSQL password",
    )

    # Logging
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = Field(
        default="INFO",
        description="Logging level",
    )

    # Quality thresholds
    max_null_rate_warning: float = Field(
        default=0.3,
        description="Null rate threshold for warnings",
    )
    max_null_rate_error: float = Field(
        default=0.5,
        description="Null rate threshold for errors",
    )

    @field_validator("project_root", mode="before")
    @classmethod
    def expand_path(cls, v: str | Path) -> Path:
        """Expand ~ and environment variables in paths."""
        if isinstance(v, str):
            v = os.path.expanduser(os.path.expandvars(v))
        return Path(v)

    @property
    def data_path(self) -> Path:
        """Path to data directory."""
        return self.project_root / "data"

    @property
    def bronze_path(self) -> Path:
        """Path to bronze layer data."""
        return self.data_path / "bronze"

    @property
    def silver_path(self) -> Path:
        """Path to silver layer data."""
        return self.data_path / "silver"

    @property
    def gold_path(self) -> Path:
        """Path to gold layer data."""
        return self.data_path / "gold"

    @property
    def diamond_path(self) -> Path:
        """Path to diamond layer data."""
        return self.data_path / "diamond"

    @property
    def quarantine_path(self) -> Path:
        """Path to quarantine data."""
        return self.data_path / "quarantine"

    @property
    def reports_path(self) -> Path:
        """Path to reports."""
        return self.data_path / "reports"

    @property
    def database_url(self) -> str:
        """PostgreSQL connection URL."""
        return (
            f"postgresql://{self.warehouse_db_user}:{self.warehouse_db_password}"
            f"@{self.warehouse_db_host}:{self.warehouse_db_port}/{self.warehouse_db_name}"
        )

    # Legacy compatibility properties
    @property
    def scrapers_unified_path(self) -> Path:
        """Legacy: Path to scrapers (now part of this project)."""
        return self.project_root

    @property
    def warehouse_path(self) -> Path:
        """Legacy: Path to warehouse (now part of this project)."""
        return self.project_root / "warehouse"
