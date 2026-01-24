"""Dashboard API for live warehouse data."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Optional

import psycopg
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import JSONResponse

from warehouse.config import config

app = FastAPI(
    title="DC Warehouse Dashboard API",
    description="Real-time analytics data for DC Procurement Warehouse",
    version="2.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)


def get_db_connection():
    """Get database connection."""
    return psycopg.connect(config.warehouse_db.connection_string)


# =============================================================================
# HEALTH CHECK
# =============================================================================

@app.get("/api/health")
def health_check():
    """Check API and database health."""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
        return {"status": "healthy", "database": "connected", "timestamp": datetime.now().isoformat()}
    except Exception as e:
        return JSONResponse(
            status_code=503,
            content={"status": "unhealthy", "database": "disconnected", "error": str(e)}
        )


# =============================================================================
# KPI ENDPOINTS
# =============================================================================

@app.get("/api/kpis")
def get_kpis() -> dict[str, Any]:
    """Get KPI summary metrics."""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM analytics.mv_kpi_summary LIMIT 1")
                row = cur.fetchone()

                # Get property count
                cur.execute("SELECT COALESCE(SUM(property_count), 0) FROM analytics.gold_property_by_ward")
                prop_row = cur.fetchone()

                if row:
                    return {
                        "totalContracts": row[1] or 0,
                        "totalSpend": float(row[2] or 0),
                        "totalVendors": row[3] or 0,
                        "avgPayment": float(row[4] or 0),
                        "cbeRate": float(row[5] or 0),
                        "propertyCount": int(prop_row[0]) if prop_row else 0,
                        "updatedAt": row[6].isoformat() if row[6] else None,
                    }
        return {}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# SPEND ENDPOINTS
# =============================================================================

@app.get("/api/spend/monthly")
def get_monthly_spend() -> dict[str, Any]:
    """Get monthly spend trend (last 12 months)."""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT year_month, total_spend
                    FROM analytics.mv_monthly_spend_trend
                    ORDER BY year_month DESC
                    LIMIT 12
                """)
                rows = cur.fetchall()[::-1]
                return {
                    "labels": [r[0] for r in rows],
                    "data": [round(float(r[1] or 0) / 1_000_000, 2) for r in rows]
                }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/spend/by-ward")
def get_spend_by_ward() -> dict[str, Any]:
    """Get spend aggregated by DC ward."""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT ward, SUM(total_spend) as total
                    FROM analytics.mv_spend_by_ward
                    GROUP BY ward
                    ORDER BY ward
                """)
                rows = cur.fetchall()
                return {
                    "labels": [r[0] for r in rows],
                    "data": [round(float(r[1] or 0) / 1_000_000, 2) for r in rows]
                }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/spend/by-agency")
def get_spend_by_agency(limit: int = 8) -> dict[str, Any]:
    """Get spend by agency (top N)."""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT agency_name_normalized, total_contract_value
                    FROM analytics.gold_agency_spend
                    ORDER BY total_contract_value DESC NULLS LAST
                    LIMIT %s
                """, (limit,))
                rows = cur.fetchall()
                return {
                    "labels": [r[0][:20] if r[0] else "Unknown" for r in rows],
                    "data": [round(float(r[1] or 0) / 1_000_000, 2) for r in rows]
                }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# VENDOR ENDPOINTS
# =============================================================================

@app.get("/api/vendors/tiers")
def get_vendor_tiers() -> dict[str, Any]:
    """Get vendor breakdown by spend tier."""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT
                        CASE
                            WHEN total_payments >= 10000000 THEN 'Enterprise (>$10M)'
                            WHEN total_payments >= 1000000 THEN 'Large ($1M-$10M)'
                            WHEN total_payments >= 100000 THEN 'Medium ($100K-$1M)'
                            ELSE 'Small (<$100K)'
                        END as tier,
                        SUM(total_payments) as total
                    FROM analytics.gold_spend_by_vendor
                    GROUP BY 1
                    ORDER BY total DESC
                """)
                rows = cur.fetchall()
                return {
                    "labels": [r[0] for r in rows],
                    "data": [round(float(r[1] or 0) / 1_000_000, 2) for r in rows]
                }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/vendors/top")
def get_top_vendors(limit: int = 10) -> list[dict[str, Any]]:
    """Get top vendors by total payments."""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT
                        vendor_id,
                        vendor_name,
                        vendor_name_normalized,
                        total_payments,
                        contract_count,
                        agency_count,
                        is_cbe,
                        CASE
                            WHEN total_payments >= 10000000 THEN 'Enterprise'
                            WHEN total_payments >= 1000000 THEN 'Large'
                            WHEN total_payments >= 100000 THEN 'Medium'
                            ELSE 'Small'
                        END as tier
                    FROM analytics.gold_spend_by_vendor
                    ORDER BY total_payments DESC NULLS LAST
                    LIMIT %s
                """, (limit,))
                rows = cur.fetchall()
                return [
                    {
                        "rank": i + 1,
                        "vendorId": r[0],
                        "name": r[1] or "Unknown",
                        "normalized": r[2] or "",
                        "payments": float(r[3] or 0),
                        "contracts": r[4] or 0,
                        "agencies": r[5] or 0,
                        "isCbe": r[6] or False,
                        "tier": r[7],
                    }
                    for i, r in enumerate(rows)
                ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/vendors/all")
def get_all_vendors(
    limit: int = Query(500, ge=1, le=5000),
    offset: int = Query(0, ge=0),
    cbe_only: bool = Query(False)
) -> list[dict[str, Any]]:
    """Get all vendors with pagination."""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                where_clause = "WHERE is_cbe = TRUE" if cbe_only else ""
                cur.execute(f"""
                    SELECT
                        vendor_id,
                        vendor_name,
                        vendor_name_normalized,
                        total_payments,
                        contract_count,
                        agency_count,
                        is_cbe,
                        first_contract_date,
                        last_payment_date,
                        CASE
                            WHEN total_payments >= 10000000 THEN 'Enterprise'
                            WHEN total_payments >= 1000000 THEN 'Large'
                            WHEN total_payments >= 100000 THEN 'Medium'
                            ELSE 'Small'
                        END as tier
                    FROM analytics.gold_spend_by_vendor
                    {where_clause}
                    ORDER BY total_payments DESC NULLS LAST
                    LIMIT %s OFFSET %s
                """, (limit, offset))
                rows = cur.fetchall()
                return [
                    {
                        "vendor_id": r[0],
                        "vendor_name": r[1] or "Unknown",
                        "vendor_name_normalized": r[2] or "",
                        "total_payments": float(r[3] or 0),
                        "contract_count": r[4] or 0,
                        "agency_count": r[5] or 0,
                        "is_cbe": r[6] or False,
                        "first_contract_date": r[7].isoformat() if r[7] else None,
                        "last_payment_date": r[8].isoformat() if r[8] else None,
                        "tier": r[9],
                    }
                    for r in rows
                ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/vendors/{vendor_id}")
def get_vendor_details(vendor_id: str) -> dict[str, Any]:
    """Get detailed information for a specific vendor."""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT
                        vendor_id,
                        vendor_name,
                        vendor_name_normalized,
                        total_contract_value,
                        total_po_value,
                        total_payments,
                        contract_count,
                        po_count,
                        payment_count,
                        agencies,
                        agency_count,
                        first_contract_date,
                        last_payment_date,
                        fiscal_years,
                        is_cbe,
                        computed_at
                    FROM analytics.gold_spend_by_vendor
                    WHERE vendor_id = %s
                """, (vendor_id,))
                row = cur.fetchone()

                if not row:
                    raise HTTPException(status_code=404, detail="Vendor not found")

                return {
                    "vendor_id": row[0],
                    "vendor_name": row[1],
                    "vendor_name_normalized": row[2],
                    "total_contract_value": float(row[3] or 0),
                    "total_po_value": float(row[4] or 0),
                    "total_payments": float(row[5] or 0),
                    "contract_count": row[6] or 0,
                    "po_count": row[7] or 0,
                    "payment_count": row[8] or 0,
                    "agencies": row[9] or [],
                    "agency_count": row[10] or 0,
                    "first_contract_date": row[11].isoformat() if row[11] else None,
                    "last_payment_date": row[12].isoformat() if row[12] else None,
                    "fiscal_years": row[13] or [],
                    "is_cbe": row[14] or False,
                    "computed_at": row[15].isoformat() if row[15] else None,
                }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/vendors/{vendor_id}/timeline")
def get_vendor_timeline(vendor_id: str) -> list[dict[str, Any]]:
    """Get monthly payment timeline for a vendor."""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Try to get from fact_spend if available
                cur.execute("""
                    SELECT
                        TO_CHAR(d.full_date, 'Mon YYYY') as month,
                        d.full_date,
                        SUM(f.spend_amount) as spend
                    FROM analytics.fact_spend f
                    JOIN analytics.dim_date d ON f.date_key = d.date_key
                    JOIN analytics.dim_supplier s ON f.supplier_key = s.supplier_key
                    WHERE s.supplier_uuid = %s
                    GROUP BY 1, 2
                    ORDER BY d.full_date DESC
                    LIMIT 24
                """, (vendor_id,))
                rows = cur.fetchall()[::-1]  # Reverse to chronological order

                if rows:
                    return [
                        {
                            "month": r[0],
                            "date": r[1].isoformat() if r[1] else None,
                            "spend": float(r[2] or 0),
                        }
                        for r in rows
                    ]

                # Fallback: return empty if no data
                return []
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# CBE ENDPOINTS
# =============================================================================

@app.get("/api/cbe/summary")
def get_cbe_summary() -> dict[str, Any]:
    """Get CBE vs Non-CBE summary statistics."""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT
                        COUNT(*) FILTER (WHERE is_cbe) as cbe_vendor_count,
                        COUNT(*) FILTER (WHERE NOT is_cbe OR is_cbe IS NULL) as non_cbe_vendor_count,
                        COALESCE(SUM(total_payments) FILTER (WHERE is_cbe), 0) as cbe_spend,
                        COALESCE(SUM(total_payments) FILTER (WHERE NOT is_cbe OR is_cbe IS NULL), 0) as non_cbe_spend
                    FROM analytics.gold_spend_by_vendor
                """)
                row = cur.fetchone()

                # Get by tier breakdown
                cur.execute("""
                    SELECT
                        CASE
                            WHEN total_payments >= 10000000 THEN 'Enterprise'
                            WHEN total_payments >= 1000000 THEN 'Large'
                            WHEN total_payments >= 100000 THEN 'Medium'
                            ELSE 'Small'
                        END as tier,
                        COUNT(*) FILTER (WHERE is_cbe) as cbe_count,
                        COUNT(*) FILTER (WHERE NOT is_cbe OR is_cbe IS NULL) as non_cbe_count
                    FROM analytics.gold_spend_by_vendor
                    GROUP BY 1
                    ORDER BY CASE tier
                        WHEN 'Enterprise' THEN 1
                        WHEN 'Large' THEN 2
                        WHEN 'Medium' THEN 3
                        ELSE 4
                    END
                """)
                tier_rows = cur.fetchall()

                return {
                    "cbeVendorCount": row[0] or 0,
                    "nonCbeVendorCount": row[1] or 0,
                    "cbeSpend": float(row[2] or 0),
                    "nonCbeSpend": float(row[3] or 0),
                    "byTier": {
                        "labels": [r[0] for r in tier_rows],
                        "cbe": [r[1] or 0 for r in tier_rows],
                        "nonCbe": [r[2] or 0 for r in tier_rows],
                    }
                }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/cbe/vendors")
def get_cbe_vendors(limit: int = 50) -> list[dict[str, Any]]:
    """Get list of CBE vendors sorted by payments."""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT
                        vendor_id,
                        vendor_name,
                        vendor_name_normalized,
                        total_payments,
                        contract_count,
                        agency_count,
                        CASE
                            WHEN total_payments >= 10000000 THEN 'Enterprise'
                            WHEN total_payments >= 1000000 THEN 'Large'
                            WHEN total_payments >= 100000 THEN 'Medium'
                            ELSE 'Small'
                        END as tier
                    FROM analytics.gold_spend_by_vendor
                    WHERE is_cbe = TRUE
                    ORDER BY total_payments DESC NULLS LAST
                    LIMIT %s
                """, (limit,))
                rows = cur.fetchall()
                return [
                    {
                        "vendor_id": r[0],
                        "vendor_name": r[1] or "Unknown",
                        "vendor_name_normalized": r[2] or "",
                        "total_payments": float(r[3] or 0),
                        "contract_count": r[4] or 0,
                        "agency_count": r[5] or 0,
                        "tier": r[6],
                    }
                    for r in rows
                ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# AGENCY ENDPOINTS
# =============================================================================

@app.get("/api/agencies/list")
def get_agencies_list() -> list[str]:
    """Get list of all agency names for filtering."""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT DISTINCT agency_name_normalized
                    FROM analytics.gold_agency_spend
                    WHERE agency_name_normalized IS NOT NULL
                    ORDER BY agency_name_normalized
                """)
                return [r[0] for r in cur.fetchall()]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/agencies/{agency_name}")
def get_agency_details(agency_name: str) -> dict[str, Any]:
    """Get detailed information for a specific agency."""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT
                        agency_name_normalized,
                        agency_name,
                        total_contract_value,
                        total_payments,
                        total_po_value,
                        contract_count,
                        vendor_count,
                        top_vendors,
                        fiscal_year_breakdown,
                        computed_at
                    FROM analytics.gold_agency_spend
                    WHERE agency_name_normalized = %s
                """, (agency_name,))
                row = cur.fetchone()

                if not row:
                    raise HTTPException(status_code=404, detail="Agency not found")

                return {
                    "agency_name_normalized": row[0],
                    "agency_name": row[1],
                    "total_contract_value": float(row[2] or 0),
                    "total_payments": float(row[3] or 0),
                    "total_po_value": float(row[4] or 0),
                    "contract_count": row[5] or 0,
                    "vendor_count": row[6] or 0,
                    "top_vendors": row[7] or [],
                    "fiscal_year_breakdown": row[8] or {},
                    "computed_at": row[9].isoformat() if row[9] else None,
                }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# REAL ESTATE ENDPOINTS
# =============================================================================

@app.get("/api/real-estate/property-by-ward")
def get_property_by_ward() -> list[dict[str, Any]]:
    """Get property statistics by DC ward."""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT
                        ward,
                        property_count,
                        total_assessed_value,
                        avg_assessed_value,
                        median_assessed_value,
                        min_assessed_value,
                        max_assessed_value,
                        avg_sqft,
                        avg_price_per_sqft,
                        avg_year_built,
                        property_type_distribution,
                        bedroom_distribution
                    FROM analytics.gold_property_by_ward
                    ORDER BY ward
                """)
                rows = cur.fetchall()
                return [
                    {
                        "ward": r[0],
                        "property_count": r[1] or 0,
                        "total_assessed_value": float(r[2] or 0),
                        "avg_assessed_value": float(r[3] or 0),
                        "median_assessed_value": float(r[4] or 0),
                        "min_assessed_value": float(r[5] or 0),
                        "max_assessed_value": float(r[6] or 0),
                        "avg_sqft": float(r[7] or 0),
                        "avg_price_per_sqft": float(r[8] or 0),
                        "avg_year_built": float(r[9] or 0),
                        "property_type_distribution": r[10] or {},
                        "bedroom_distribution": r[11] or {},
                    }
                    for r in rows
                ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/real-estate/market-trends")
def get_market_trends(
    metric_name: Optional[str] = None,
    region_type: Optional[str] = None
) -> list[dict[str, Any]]:
    """Get market trend data (Zillow, RentCast, etc.)."""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                query = """
                    SELECT
                        region_name,
                        region_type,
                        metric_name,
                        metric_category,
                        latest_value,
                        latest_date,
                        yoy_change,
                        mom_change,
                        trend_direction,
                        historical_values
                    FROM analytics.gold_market_trends
                    WHERE 1=1
                """
                params = []

                if metric_name:
                    query += " AND metric_name = %s"
                    params.append(metric_name)

                if region_type:
                    query += " AND region_type = %s"
                    params.append(region_type)

                query += " ORDER BY metric_name, region_name"

                cur.execute(query, params)
                rows = cur.fetchall()
                return [
                    {
                        "region_name": r[0],
                        "region_type": r[1],
                        "metric_name": r[2],
                        "metric_category": r[3],
                        "latest_value": float(r[4] or 0),
                        "latest_date": r[5].isoformat() if r[5] else None,
                        "yoy_change": float(r[6] or 0),
                        "mom_change": float(r[7] or 0),
                        "trend_direction": r[8],
                        "historical_values": r[9] or [],
                    }
                    for r in rows
                ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# COMBINED DATA ENDPOINT
# =============================================================================

@app.get("/api/all")
def get_all_dashboard_data() -> dict[str, Any]:
    """Get all dashboard data in a single request."""
    return {
        "kpis": get_kpis(),
        "monthlySpend": get_monthly_spend(),
        "spendByWard": get_spend_by_ward(),
        "agencySpend": get_spend_by_agency(),
        "vendorTiers": get_vendor_tiers(),
        "topVendors": get_top_vendors(),
        "exportedAt": datetime.now().isoformat(),
    }


# =============================================================================
# STATIC FILES (Must be last)
# =============================================================================

dashboard_dir = Path(__file__).parent / "dashboard"
if dashboard_dir.exists():
    app.mount("/", StaticFiles(directory=dashboard_dir, html=True), name="dashboard")
