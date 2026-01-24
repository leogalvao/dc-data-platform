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

    Joins to dimension tables to get surrogate keys (supplier_key, contract_key).
    """

    source_pattern = "source=dc_payments"
    target_table = "analytics.fact_spend"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._supplier_cache: dict[str, int] = {}
        self._contract_cache: dict[str, int] = {}

    def _build_lookup_caches(self) -> None:
        """Build lookup caches for dimension keys."""
        with self.get_cursor() as cursor:
            # Supplier cache: normalized_name -> supplier_key
            cursor.execute(
                """
                SELECT supplier_key, UPPER(TRIM(supplier_name))
                FROM analytics.dim_supplier
                WHERE is_current = TRUE
                """
            )
            self._supplier_cache = {row[1]: row[0] for row in cursor.fetchall() if row[1]}

            # Contract cache: contract_number -> contract_key
            cursor.execute(
                """
                SELECT contract_key, contract_number
                FROM analytics.dim_contract
                WHERE is_current = TRUE
                """
            )
            self._contract_cache = {row[1]: row[0] for row in cursor.fetchall() if row[1]}

        self.logger.info(
            f"Built caches: {len(self._supplier_cache)} suppliers, "
            f"{len(self._contract_cache)} contracts"
        )

    def _normalize_supplier_name(self, name: str | None) -> str | None:
        """Normalize supplier name for lookup."""
        if not name:
            return None
        return name.upper().strip()

    def _get_date_key(self, payment_date: Any) -> int | None:
        """Convert payment date to date_key (YYYYMMDD format)."""
        if payment_date is None:
            return None

        if isinstance(payment_date, str):
            try:
                dt = datetime.fromisoformat(payment_date.replace("Z", "+00:00"))
                return int(dt.strftime("%Y%m%d"))
            except (ValueError, TypeError):
                return None

        if isinstance(payment_date, datetime):
            return int(payment_date.strftime("%Y%m%d"))

        if hasattr(payment_date, "strftime"):
            return int(payment_date.strftime("%Y%m%d"))

        return None

    def _get_fiscal_year(self, payment_date: Any) -> int | None:
        """Get DC fiscal year (Oct 1 - Sep 30)."""
        if payment_date is None:
            return None

        if isinstance(payment_date, str):
            try:
                dt = datetime.fromisoformat(payment_date.replace("Z", "+00:00"))
            except (ValueError, TypeError):
                return None
        elif isinstance(payment_date, datetime):
            dt = payment_date
        elif hasattr(payment_date, "month"):
            dt = payment_date
        else:
            return None

        if dt.month >= 10:
            return dt.year + 1
        return dt.year

    def _get_fiscal_quarter(self, payment_date: Any) -> int | None:
        """Get DC fiscal quarter."""
        if payment_date is None:
            return None

        if isinstance(payment_date, str):
            try:
                dt = datetime.fromisoformat(payment_date.replace("Z", "+00:00"))
                month = dt.month
            except (ValueError, TypeError):
                return None
        elif hasattr(payment_date, "month"):
            month = payment_date.month
        else:
            return None

        if month in (10, 11, 12):
            return 1
        elif month in (1, 2, 3):
            return 2
        elif month in (4, 5, 6):
            return 3
        else:
            return 4

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
                    supplier_key = self._supplier_cache.get(supplier_name) if supplier_name else None

                    if not supplier_key:
                        skipped_no_supplier += 1
                        continue

                    payment_date = r.get("payment_date")
                    date_key = self._get_date_key(payment_date)

                    if not date_key:
                        skipped_no_date += 1
                        continue

                    contract_num = r.get("contract_number")
                    contract_key = self._contract_cache.get(contract_num) if contract_num else None

                    fiscal_year = r.get("fiscal_year") or self._get_fiscal_year(payment_date)
                    fiscal_quarter = self._get_fiscal_quarter(payment_date)

                    # Generate a deterministic UUID from source_record_id
                    source_id = r.get("source_record_id")
                    if source_id:
                        payment_uuid = str(uuid_module.uuid5(
                            uuid_module.NAMESPACE_DNS,
                            f"dc_payments:{source_id}"
                        ))
                    else:
                        payment_uuid = None

                    transformed.append({
                        "date_key": date_key,
                        "supplier_key": supplier_key,
                        "contract_key": contract_key,
                        "payment_uuid": payment_uuid,
                        "spend_amount": r.get("payment_amount"),
                        "payment_type": r.get("payment_type"),
                        "fiscal_year": fiscal_year,
                        "fiscal_quarter": fiscal_quarter,
                    })

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
            f"Skipped: {skipped_no_supplier} (no supplier), {skipped_no_date} (no date)"
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
                            date_key, supplier_key, contract_key,
                            payment_uuid, spend_amount, payment_type,
                            fiscal_year, fiscal_quarter, loaded_at
                        ) VALUES (
                            %(date_key)s, %(supplier_key)s, %(contract_key)s,
                            %(payment_uuid)s, %(spend_amount)s, %(payment_type)s,
                            %(fiscal_year)s, %(fiscal_quarter)s, NOW()
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
