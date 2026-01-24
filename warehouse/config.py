"""Configuration for warehouse data loading."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

# Load .env file if present
load_dotenv()


@dataclass
class DatabaseConfig:
    """PostgreSQL database connection configuration."""

    host: str
    port: int
    database: str
    user: str
    password: str

    @property
    def connection_string(self) -> str:
        """Return psycopg connection string."""
        return f"host={self.host} port={self.port} dbname={self.database} user={self.user} password={self.password}"

    @property
    def dsn(self) -> str:
        """Return DSN format connection string."""
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"


@dataclass
class Config:
    """Application configuration."""

    # Paths
    scrapers_unified_path: Path
    silver_path: Path
    gold_path: Path

    # Database
    warehouse_db: DatabaseConfig

    # Batch settings
    batch_size: int = 1000

    @classmethod
    def from_env(cls) -> "Config":
        """Load configuration from environment variables."""
        scrapers_path = Path(
            os.environ.get(
                "SCRAPERS_UNIFIED_PATH",
                "/home/lgalvao/OneDrive/Scripts/scrapers_unified/data",
            )
        )

        return cls(
            scrapers_unified_path=scrapers_path,
            silver_path=scrapers_path / "silver",
            gold_path=scrapers_path / "gold",
            warehouse_db=DatabaseConfig(
                host=os.environ.get("WAREHOUSE_DB_HOST", "localhost"),
                port=int(os.environ.get("WAREHOUSE_DB_PORT", "5433")),
                database=os.environ.get("WAREHOUSE_DB_NAME", "procurement_warehouse"),
                user=os.environ.get("WAREHOUSE_DB_USER", "warehouse_user"),
                password=os.environ.get("WAREHOUSE_DB_PASSWORD", "change_me_warehouse"),
            ),
            batch_size=int(os.environ.get("BATCH_SIZE", "1000")),
        )


# Global config instance
config = Config.from_env()
