"""Load dim_supplier from Silver Parquet data."""

from __future__ import annotations

import uuid
from typing import Any

import pyarrow.parquet as pq

from .base import BaseParquetLoader


class DimSupplierLoader(BaseParquetLoader):
    """
    Load supplier dimension from multiple Silver sources.

    Sources:
        - dc_cbe_businesses -> is_cbe=True, cbe_categories
        - dc_contracts -> supplier_name (all vendors)
        - dc_payments -> supplier_name (all vendors)

    Deduplication is done by normalized vendor name.
    """

    source_pattern = "source=dc_cbe_businesses"  # Primary source
    target_table = "analytics.dim_supplier"

    def read_source_data(self) -> list[dict[str, Any]]:
        """
        Read from multiple sources and merge.

        Priority:
            1. CBE businesses (authoritative for CBE status)
            2. Contracts (vendor info)
            3. Payments (vendor info)
        """
        vendors: dict[str, dict[str, Any]] = {}  # normalized_name -> record

        # 1. Load CBE businesses (authoritative for CBE status)
        cbe_path = self.parquet_path / "source=dc_cbe_businesses"
        if cbe_path.exists():
            for pq_file in cbe_path.rglob("*.parquet"):
                try:
                    table = pq.read_table(pq_file)
                    for row in table.to_pylist():
                        name = self._normalize_name(row.get("business_name"))
                        if name:
                            vendors[name] = {
                                "supplier_name": row.get("business_name"),
                                "dba_name": row.get("trade_name"),
                                "business_address": row.get("address"),
                                "city": row.get("city", "Washington"),
                                "state": row.get("state", "DC"),
                                "zip_code": row.get("zip_code"),
                                "ward": row.get("ward"),
                                "is_cbe": True,
                                "cbe_categories": self._parse_cbe_categories(
                                    row.get("certification_type")
                                ),
                                "registration_date": row.get("registration_date"),
                            }
                except Exception as e:
                    self.logger.warning(f"Error reading CBE file {pq_file}: {e}")

        self.logger.info(f"Loaded {len(vendors)} CBE businesses")

        # 2. Load vendors from contracts (may not be CBE)
        contracts_path = self.parquet_path / "source=dc_contracts"
        if contracts_path.exists():
            contract_vendors = 0
            for pq_file in contracts_path.rglob("*.parquet"):
                try:
                    table = pq.read_table(pq_file)
                    for row in table.to_pylist():
                        name = self._normalize_name(row.get("supplier_name"))
                        if name and name not in vendors:
                            vendors[name] = {
                                "supplier_name": row.get("supplier_name"),
                                "is_cbe": False,
                            }
                            contract_vendors += 1
                except Exception as e:
                    self.logger.warning(f"Error reading contract file {pq_file}: {e}")

            self.logger.info(f"Added {contract_vendors} vendors from contracts")

        # 3. Load vendors from payments
        payments_path = self.parquet_path / "source=dc_payments"
        if payments_path.exists():
            payment_vendors = 0
            for pq_file in payments_path.rglob("*.parquet"):
                try:
                    table = pq.read_table(pq_file)
                    for row in table.to_pylist():
                        name = self._normalize_name(row.get("supplier_name"))
                        if name and name not in vendors:
                            vendors[name] = {
                                "supplier_name": row.get("supplier_name"),
                                "is_cbe": False,
                            }
                            payment_vendors += 1
                except Exception as e:
                    self.logger.warning(f"Error reading payment file {pq_file}: {e}")

            self.logger.info(f"Added {payment_vendors} vendors from payments")

        self.logger.info(f"Found {len(vendors)} unique vendors total")
        return list(vendors.values())

    def _normalize_name(self, name: str | None) -> str | None:
        """Normalize vendor name for deduplication."""
        if not name:
            return None

        normalized = name.upper().strip()

        # Remove common suffixes for deduplication
        suffixes = [
            ", LLC", " LLC", ", L.L.C.", " L.L.C.",
            ", INC", " INC", ", INC.", " INC.",
            ", CORP", " CORP", ", CORP.", " CORP.",
            ", INCORPORATED", " INCORPORATED",
            ", COMPANY", " COMPANY", ", CO", " CO", ", CO.", " CO.",
        ]

        for suffix in suffixes:
            if normalized.endswith(suffix):
                normalized = normalized[: -len(suffix)]
                break

        return normalized.strip()

    def _parse_cbe_categories(self, cert_type: str | None) -> list[str]:
        """Parse CBE certification categories from string."""
        if not cert_type:
            return []

        if ";" in cert_type:
            return [c.strip() for c in cert_type.split(";") if c.strip()]
        elif "," in cert_type:
            return [c.strip() for c in cert_type.split(",") if c.strip()]
        else:
            return [cert_type.strip()] if cert_type.strip() else []

    def transform(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Add UUIDs and clean data."""
        transformed = []

        for r in records:
            supplier_name = r.get("supplier_name")
            if not supplier_name:
                continue

            transformed.append({
                "supplier_uuid": str(uuid.uuid4()),
                "supplier_name": supplier_name,
                "dba_name": r.get("dba_name"),
                "business_address": r.get("business_address"),
                "city": r.get("city"),
                "state": r.get("state"),
                "zip_code": r.get("zip_code"),
                "is_cbe": r.get("is_cbe", False),
                "cbe_categories": r.get("cbe_categories", []),
                "registration_date": r.get("registration_date"),
            })

        return transformed

    def load(self, records: list[dict[str, Any]]) -> dict[str, Any]:
        """Upsert into dim_supplier."""
        inserted = 0
        updated = 0

        with self.get_cursor() as cursor:
            for r in records:
                cursor.execute(
                    """
                    INSERT INTO analytics.dim_supplier (
                        supplier_uuid, supplier_name, dba_name,
                        business_address, city, state, zip_code,
                        is_cbe, cbe_categories, registration_date,
                        updated_at
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW()
                    )
                    ON CONFLICT (supplier_uuid) DO UPDATE SET
                        supplier_name = EXCLUDED.supplier_name,
                        dba_name = EXCLUDED.dba_name,
                        business_address = EXCLUDED.business_address,
                        city = EXCLUDED.city,
                        state = EXCLUDED.state,
                        zip_code = EXCLUDED.zip_code,
                        is_cbe = EXCLUDED.is_cbe,
                        cbe_categories = EXCLUDED.cbe_categories,
                        updated_at = NOW()
                    RETURNING (xmax = 0) AS inserted
                    """,
                    (
                        r["supplier_uuid"],
                        r["supplier_name"],
                        r.get("dba_name"),
                        r.get("business_address"),
                        r.get("city"),
                        r.get("state"),
                        r.get("zip_code"),
                        r.get("is_cbe", False),
                        r.get("cbe_categories", []),
                        r.get("registration_date"),
                    ),
                )

                row = cursor.fetchone()
                if row and row[0]:
                    inserted += 1
                else:
                    updated += 1

        return {
            "table": self.target_table,
            "inserted": inserted,
            "updated": updated,
            "total": len(records),
        }
