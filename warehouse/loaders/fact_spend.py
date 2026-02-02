"""Load fact_spend from Silver payments data."""

from __future__ import annotations

import uuid as uuid_module
from datetime import datetime
from typing import Any

from warehouse.config import config

from .base import BaseParquetLoader


class FactSpendLoader(BaseParquetLoader):
    """
    Load fact_spend from dc_payments Silver data.

    Joins to dimension tables to get surrogate keys (supplier_id, contract_id).
    """

    source_pattern = "source=dc_payments"
    target_table = "analytics.fact_spend"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._supplier_cache: dict[str, str] = {}
        self._contract_cache: dict[str, str] = {}

    def _build_lookup_caches(self) -> None:
        """Build lookup caches for dimension keys."""
        with self.get_cursor() as cursor:
            # Supplier cache: normalized_name -> supplier_id
            cursor.execute(
                """
                SELECT supplier_id, UPPER(TRIM(supplier_name))
                FROM analytics.dim_supplier
                """
            )
            self._supplier_cache = {row[1]: str(row[0]) for row in cursor.fetchall() if row[1]}

            # Contract cache: contract_number -> contract_id
            cursor.execute(
                """
                SELECT contract_id, contract_number
                FROM analytics.dim_contract
                """
            )
            self._contract_cache = {row[1]: str(row[0]) for row in cursor.fetchall() if row[1]}

        self.logger.info(
            f"Built caches: {len(self._supplier_cache)} suppliers, "
            f"{len(self._contract_cache)} contracts"
        )

    def _normalize_supplier_name(self, name: str | None) -> str | None:
        """Normalize supplier name for lookup."""
        if not name:
            return None
        return name.upper().strip()

    def _parse_payment_date(self, payment_date: Any) -> datetime | None:
        """Parse payment date to datetime."""
        if payment_date is None:
            return None

        if isinstance(payment_date, str):
            try:
                return datetime.fromisoformat(payment_date.replace("Z", "+00:00"))
            except (ValueError, TypeError):
                return None

        if isinstance(payment_date, datetime):
            return payment_date

        if hasattr(payment_date, "strftime"):
            return payment_date

        return None

    def _get_fiscal_year(self, payment_date: Any) -> int | None:
        """Get DC fiscal year (Oct 1 - Sep 30)."""
        dt = self._parse_payment_date(payment_date)
        if dt is None:
            return None

        if dt.month >= 10:
            return dt.year + 1
        return dt.year

    def _get_fiscal_quarter(self, payment_date: Any) -> int | None:
        """Get DC fiscal quarter."""
        dt = self._parse_payment_date(payment_date)
        if dt is None:
            return None

        month = dt.month
        if month in (10, 11, 12):
            return 1
        elif month in (1, 2, 3):
            return 2
        elif month in (4, 5, 6):
            return 3
        else:
            return 4

    def _get_fiscal_month(self, payment_date: Any) -> int | None:
        """Get DC fiscal month (1-12, starting from October)."""
        dt = self._parse_payment_date(payment_date)
        if dt is None:
            return None

        month = dt.month
        if month >= 10:
            return month - 9
        return month + 3

    def run(self) -> dict[str, Any]:
        """
        Execute ETL by processing files incrementally to avoid memory issues.

        Overrides base class to handle large datasets file-by-file.
        """
        self.logger.info(f"Starting load: {self.source_pattern} -> {self.target_table}")

        # Build caches first
        self._build_lookup_caches()

        source_path = self.parquet_path / self.source_pattern
        if not source_path.exists():
            return {"status": "skipped", "reason": "no source data", "table": self.target_table}

        parquet_files = sorted(source_path.rglob("*.parquet"))
        if not parquet_files:
            return {"status": "skipped", "reason": "no parquet files", "table": self.target_table}

        total_inserted = 0
        total_raw = 0
        total_transformed = 0
        skipped_no_supplier = 0
        skipped_no_date = 0

        import pyarrow.parquet as pq

        for i, pq_file in enumerate(parquet_files):
            try:
                table = pq.read_table(pq_file)
                records = table.to_pylist()
                total_raw += len(records)

                # Transform this batch
                transformed = []
                for r in records:
                    supplier_name = self._normalize_supplier_name(r.get("supplier_name"))
                    supplier_id = self._supplier_cache.get(supplier_name) if supplier_name else None

                    payment_date = r.get("payment_date")
                    parsed_date = self._parse_payment_date(payment_date)

                    if not parsed_date:
                        skipped_no_date += 1
                        continue

                    contract_num = r.get("contract_number")
                    contract_id = self._contract_cache.get(contract_num) if contract_num else None

                    fiscal_year = r.get("fiscal_year") or self._get_fiscal_year(payment_date)
                    fiscal_quarter = self._get_fiscal_quarter(payment_date)
                    fiscal_month = self._get_fiscal_month(payment_date)

                    # Generate a deterministic UUID from source_record_id
                    source_id = r.get("source_record_id")
                    if source_id:
                        payment_uuid = str(uuid_module.uuid5(
                            uuid_module.NAMESPACE_DNS,
                            f"dc_payments:{source_id}"
                        ))
                    else:
                        payment_uuid = str(uuid_module.uuid4())

                    transformed.append({
                        "payment_uuid": payment_uuid,
                        "supplier_id": supplier_id,
                        "contract_id": contract_id,
                        "fiscal_year": fiscal_year,
                        "fiscal_quarter": fiscal_quarter,
                        "fiscal_month": fiscal_month,
                        "payment_date": parsed_date.date() if parsed_date else None,
                        "spend_amount": r.get("payment_amount"),
                        "payment_type": r.get("payment_type"),
                        "agency_code": r.get("agency_code"),
                        "agency_name": r.get("agency_name"),
                        "fund_type": r.get("fund_type"),
                        "appropriation": r.get("appropriation"),
                        "cost_center": r.get("cost_center"),
                        "vendor_name": r.get("supplier_name"),
                    })

                    if not supplier_id:
                        skipped_no_supplier += 1

                total_transformed += len(transformed)

                # Load this batch
                if transformed:
                    inserted = self._load_batch(transformed)
                    total_inserted += inserted

                self.logger.info(
                    f"File {i+1}/{len(parquet_files)}: {len(records)} raw -> "
                    f"{len(transformed)} transformed -> {inserted if transformed else 0} inserted"
                )

                # Clear memory
                del records
                del transformed
                del table

            except Exception as e:
                self.logger.warning(f"Error processing {pq_file.name}: {e}")

        self.logger.info(
            f"Total: {total_raw} raw, {total_transformed} transformed, {total_inserted} inserted. "
            f"Skipped: {skipped_no_supplier} (no supplier match), {skipped_no_date} (no date)"
        )

        return {
            "status": "success",
            "table": self.target_table,
            "inserted": total_inserted,
            "raw_count": total_raw,
            "transformed_count": total_transformed,
        }

    def _load_batch(self, records: list[dict[str, Any]]) -> int:
        """Load a batch of records into fact_spend."""
        if not records:
            return 0

        inserted = 0
        batch_size = config.batch_size

        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                for i in range(0, len(records), batch_size):
                    batch = records[i : i + batch_size]

                    cursor.executemany(
                        """
                        INSERT INTO analytics.fact_spend (
                            payment_uuid, supplier_id, contract_id,
                            fiscal_year, fiscal_quarter, fiscal_month,
                            payment_date, spend_amount, payment_type,
                            agency_code, agency_name, fund_type,
                            appropriation, cost_center, vendor_name
                        ) VALUES (
                            %(payment_uuid)s, %(supplier_id)s, %(contract_id)s,
                            %(fiscal_year)s, %(fiscal_quarter)s, %(fiscal_month)s,
                            %(payment_date)s, %(spend_amount)s, %(payment_type)s,
                            %(agency_code)s, %(agency_name)s, %(fund_type)s,
                            %(appropriation)s, %(cost_center)s, %(vendor_name)s
                        )
                        ON CONFLICT DO NOTHING
                        """,
                        batch,
                    )
                    inserted += cursor.rowcount

        return inserted

    def transform(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Not used - processing done in run() for memory efficiency."""
        return records

    def load(self, records: list[dict[str, Any]]) -> dict[str, Any]:
        """Not used - processing done in run() for memory efficiency."""
        return {"inserted": 0}
