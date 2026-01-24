"""Load dim_contract from Silver Parquet data."""

from __future__ import annotations

import uuid
from typing import Any

from .base import BaseParquetLoader


class DimContractLoader(BaseParquetLoader):
    """
    Load contract dimension from dc_contracts Silver data.

    Deduplicates by contract_number (keeps most complete record).
    """

    source_pattern = "source=dc_contracts"
    target_table = "analytics.dim_contract"

    def transform(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """
        Deduplicate and transform contracts.

        DC contracts data may have multiple records per contract_number,
        so we deduplicate keeping the most complete record.
        """
        seen: dict[str, dict[str, Any]] = {}  # contract_number -> best record

        for r in records:
            contract_num = r.get("contract_number")
            if not contract_num:
                continue

            # Keep the record with more filled fields
            if contract_num not in seen:
                seen[contract_num] = r
            else:
                existing = seen[contract_num]
                new_filled = sum(1 for v in r.values() if v is not None)
                old_filled = sum(1 for v in existing.values() if v is not None)
                if new_filled > old_filled:
                    seen[contract_num] = r

        transformed = []
        for contract_num, r in seen.items():
            nigp_codes = self._parse_codes(r.get("nigp_codes"))
            naics_codes = self._parse_codes(r.get("naics_codes"))

            transformed.append({
                "contract_uuid": str(uuid.uuid4()),
                "contract_number": contract_num,
                "contract_title": r.get("title") or r.get("description"),
                "contract_type": r.get("contract_type"),
                "procurement_method": r.get("procurement_method"),
                "nigp_codes": nigp_codes,
                "naics_codes": naics_codes,
                "agency_code": r.get("agency_code"),
                "agency_name": r.get("agency_name"),
            })

        self.logger.info(
            f"Deduplicated {len(records)} records to {len(transformed)} unique contracts"
        )
        return transformed

    def _parse_codes(self, codes: Any) -> list[str]:
        """Parse NIGP/NAICS codes from various formats."""
        if not codes:
            return []

        if isinstance(codes, list):
            return [str(c) for c in codes if c]

        if isinstance(codes, str):
            if ";" in codes:
                return [c.strip() for c in codes.split(";") if c.strip()]
            elif "," in codes:
                return [c.strip() for c in codes.split(",") if c.strip()]
            else:
                return [codes.strip()] if codes.strip() else []

        return []

    def load(self, records: list[dict[str, Any]]) -> dict[str, Any]:
        """Upsert into dim_contract."""
        inserted = 0
        updated = 0

        with self.get_cursor() as cursor:
            for r in records:
                cursor.execute(
                    """
                    INSERT INTO analytics.dim_contract (
                        contract_uuid, contract_number, contract_title,
                        contract_type, procurement_method,
                        nigp_codes, naics_codes,
                        agency_code, agency_name,
                        updated_at
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW()
                    )
                    ON CONFLICT (contract_uuid) DO UPDATE SET
                        contract_number = EXCLUDED.contract_number,
                        contract_title = EXCLUDED.contract_title,
                        contract_type = EXCLUDED.contract_type,
                        procurement_method = EXCLUDED.procurement_method,
                        nigp_codes = EXCLUDED.nigp_codes,
                        naics_codes = EXCLUDED.naics_codes,
                        agency_code = EXCLUDED.agency_code,
                        agency_name = EXCLUDED.agency_name,
                        updated_at = NOW()
                    RETURNING (xmax = 0) AS inserted
                    """,
                    (
                        r["contract_uuid"],
                        r["contract_number"],
                        r.get("contract_title"),
                        r.get("contract_type"),
                        r.get("procurement_method"),
                        r.get("nigp_codes", []),
                        r.get("naics_codes", []),
                        r.get("agency_code"),
                        r.get("agency_name"),
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
