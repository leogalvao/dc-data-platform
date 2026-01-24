"""Load dim_geography from Silver Parquet data."""

from __future__ import annotations

from typing import Any

import pyarrow.parquet as pq

from .base import BaseParquetLoader


class DimGeographyLoader(BaseParquetLoader):
    """
    Load geography dimension from property and CBE data.

    Sources:
        - dc_residential_cama (PropertyRecord) -> ward, zip_code, lat/lon
        - dc_cbe_businesses (BusinessRecord) -> ward, zip_code

    Creates unique geography records for DC wards and zip codes.
    """

    source_pattern = "source=dc_cbe_businesses"
    target_table = "analytics.dim_geography"

    def read_source_data(self) -> list[dict[str, Any]]:
        """Read geography info from multiple sources."""
        geographies: dict[str, dict[str, Any]] = {}  # ward_zip -> record

        # 1. Load from property data (has best geo coordinates)
        property_sources = [
            "source=dc_residential_cama",
            "source=crossing_dc",
        ]

        for source in property_sources:
            source_path = self.parquet_path / source
            if source_path.exists():
                for pq_file in source_path.rglob("*.parquet"):
                    try:
                        table = pq.read_table(pq_file)
                        for row in table.to_pylist():
                            ward = row.get("ward")
                            zip_code = row.get("zip_code")

                            if not ward and not zip_code:
                                continue

                            key = f"{ward or 'Unknown'}_{zip_code or ''}"
                            if key not in geographies:
                                geographies[key] = {
                                    "ward": ward,
                                    "zip_code": zip_code,
                                    "latitude": row.get("latitude"),
                                    "longitude": row.get("longitude"),
                                    "neighborhood": row.get("neighborhood"),
                                }
                            else:
                                existing = geographies[key]
                                if row.get("latitude") and not existing.get("latitude"):
                                    existing["latitude"] = row.get("latitude")
                                    existing["longitude"] = row.get("longitude")
                                if row.get("neighborhood") and not existing.get("neighborhood"):
                                    existing["neighborhood"] = row.get("neighborhood")
                    except Exception as e:
                        self.logger.warning(f"Error reading {pq_file}: {e}")

        # 2. Load from CBE businesses
        cbe_path = self.parquet_path / "source=dc_cbe_businesses"
        if cbe_path.exists():
            for pq_file in cbe_path.rglob("*.parquet"):
                try:
                    table = pq.read_table(pq_file)
                    for row in table.to_pylist():
                        ward = row.get("ward")
                        zip_code = row.get("zip_code")

                        if not ward and not zip_code:
                            continue

                        key = f"{ward or 'Unknown'}_{zip_code or ''}"
                        if key not in geographies:
                            geographies[key] = {
                                "ward": ward,
                                "zip_code": zip_code,
                                "latitude": row.get("latitude"),
                                "longitude": row.get("longitude"),
                            }
                except Exception as e:
                    self.logger.warning(f"Error reading CBE file {pq_file}: {e}")

        self.logger.info(f"Found {len(geographies)} unique geography combinations")
        return list(geographies.values())

    def transform(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Clean and validate geography records."""
        transformed = []

        for r in records:
            ward = r.get("ward")
            zip_code = r.get("zip_code")

            # Normalize ward format
            if ward:
                ward = str(ward).strip()
                if ward.isdigit():
                    ward = f"Ward {ward}"

            # Validate DC zip codes
            if zip_code:
                zip_code = str(zip_code).strip()[:5]
                if not zip_code.startswith("20"):
                    zip_code = None

            transformed.append({
                "ward": ward,
                "zip_code": zip_code,
                "neighborhood": r.get("neighborhood"),
                "latitude": r.get("latitude"),
                "longitude": r.get("longitude"),
            })

        return transformed

    def load(self, records: list[dict[str, Any]]) -> dict[str, Any]:
        """Insert new geography records."""
        inserted = 0
        skipped = 0

        with self.get_cursor() as cursor:
            for r in records:
                # Check if exists
                cursor.execute(
                    """
                    SELECT geography_key FROM analytics.dim_geography
                    WHERE ward = %s AND zip_code = %s
                    """,
                    (r.get("ward"), r.get("zip_code")),
                )

                if cursor.fetchone():
                    skipped += 1
                    continue

                cursor.execute(
                    """
                    INSERT INTO analytics.dim_geography (
                        ward, neighborhood, zip_code,
                        latitude, longitude,
                        created_at
                    ) VALUES (
                        %s, %s, %s, %s, %s, NOW()
                    )
                    """,
                    (
                        r.get("ward"),
                        r.get("neighborhood"),
                        r.get("zip_code"),
                        r.get("latitude"),
                        r.get("longitude"),
                    ),
                )
                inserted += 1

        return {
            "table": self.target_table,
            "inserted": inserted,
            "skipped": skipped,
            "total": len(records),
        }
