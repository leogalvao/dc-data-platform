"""Dashboard API for live warehouse data."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

import psycopg
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import JSONResponse, HTMLResponse

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
                # Get supplier stats
                cur.execute("""
                    SELECT
                        COUNT(*) as total_suppliers,
                        COUNT(*) FILTER (WHERE is_cbe = TRUE) as cbe_suppliers
                    FROM analytics.dim_supplier
                """)
                supplier_row = cur.fetchone()

                # Get contract stats
                cur.execute("SELECT COUNT(*) FROM analytics.dim_contract")
                contract_count = cur.fetchone()[0]

                # Get spend stats
                cur.execute("""
                    SELECT
                        COUNT(*) as payment_count,
                        COALESCE(SUM(spend_amount), 0) as total_spend,
                        COALESCE(AVG(spend_amount), 0) as avg_payment
                    FROM analytics.fact_spend
                """)
                spend_row = cur.fetchone()

                total_suppliers = supplier_row[0] or 0
                cbe_suppliers = supplier_row[1] or 0
                cbe_rate = (cbe_suppliers / total_suppliers * 100) if total_suppliers > 0 else 0

                return {
                    "totalSuppliers": total_suppliers,
                    "cbeSuppliers": cbe_suppliers,
                    "totalContracts": contract_count or 0,
                    "totalPayments": spend_row[0] or 0,
                    "totalSpend": float(spend_row[1] or 0),
                    "avgPayment": float(spend_row[2] or 0),
                    "cbeRate": round(cbe_rate, 2),
                    "updatedAt": datetime.now().isoformat(),
                }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# SPEND ENDPOINTS
# =============================================================================

@app.get("/api/spend/by-fiscal-year")
def get_spend_by_fiscal_year() -> dict[str, Any]:
    """Get spend by fiscal year."""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT
                        fiscal_year,
                        SUM(spend_amount) as total
                    FROM analytics.fact_spend
                    WHERE fiscal_year IS NOT NULL
                    GROUP BY fiscal_year
                    ORDER BY fiscal_year
                """)
                rows = cur.fetchall()
                return {
                    "labels": [str(r[0]) for r in rows],
                    "data": [round(float(r[1] or 0) / 1_000_000, 2) for r in rows]
                }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/spend/by-agency")
def get_spend_by_agency(limit: int = 10) -> dict[str, Any]:
    """Get spend by agency (top N)."""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT
                        COALESCE(agency_name, 'Unknown') as agency,
                        SUM(spend_amount) as total
                    FROM analytics.fact_spend
                    GROUP BY agency_name
                    ORDER BY total DESC NULLS LAST
                    LIMIT %s
                """, (limit,))
                rows = cur.fetchall()
                return {
                    "labels": [r[0][:30] if r[0] else "Unknown" for r in rows],
                    "data": [round(float(r[1] or 0) / 1_000_000, 2) for r in rows]
                }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# SUPPLIER ENDPOINTS
# =============================================================================

@app.get("/api/suppliers")
def get_suppliers(
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    cbe_only: bool = Query(False)
) -> list[dict[str, Any]]:
    """Get suppliers with pagination."""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                where_clause = "WHERE is_cbe = TRUE" if cbe_only else ""
                cur.execute(f"""
                    SELECT
                        supplier_id,
                        supplier_name,
                        dba_name,
                        city,
                        state,
                        zip_code,
                        is_cbe,
                        cbe_categories,
                        registration_date
                    FROM analytics.dim_supplier
                    {where_clause}
                    ORDER BY supplier_name
                    LIMIT %s OFFSET %s
                """, (limit, offset))
                rows = cur.fetchall()
                return [
                    {
                        "supplier_id": str(r[0]),
                        "supplier_name": r[1] or "Unknown",
                        "dba_name": r[2],
                        "city": r[3],
                        "state": r[4],
                        "zip_code": r[5],
                        "is_cbe": r[6] or False,
                        "cbe_categories": r[7] or [],
                        "registration_date": r[8].isoformat() if r[8] else None,
                    }
                    for r in rows
                ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/suppliers/top")
def get_top_suppliers(limit: int = 10) -> list[dict[str, Any]]:
    """Get top suppliers by spend amount."""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT
                        s.supplier_id,
                        s.supplier_name,
                        s.is_cbe,
                        COUNT(f.spend_id) as payment_count,
                        COALESCE(SUM(f.spend_amount), 0) as total_spend
                    FROM analytics.dim_supplier s
                    LEFT JOIN analytics.fact_spend f ON s.supplier_id = f.supplier_id
                    GROUP BY s.supplier_id, s.supplier_name, s.is_cbe
                    ORDER BY total_spend DESC NULLS LAST
                    LIMIT %s
                """, (limit,))
                rows = cur.fetchall()
                return [
                    {
                        "rank": i + 1,
                        "supplier_id": str(r[0]),
                        "name": r[1] or "Unknown",
                        "is_cbe": r[2] or False,
                        "payment_count": r[3] or 0,
                        "total_spend": float(r[4] or 0),
                    }
                    for i, r in enumerate(rows)
                ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# CONTRACT ENDPOINTS
# =============================================================================

@app.get("/api/contracts")
def get_contracts(
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0)
) -> list[dict[str, Any]]:
    """Get contracts with pagination."""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT
                        contract_id,
                        contract_number,
                        contract_title,
                        contract_type,
                        procurement_method,
                        agency_name,
                        nigp_codes,
                        naics_codes
                    FROM analytics.dim_contract
                    ORDER BY contract_number
                    LIMIT %s OFFSET %s
                """, (limit, offset))
                rows = cur.fetchall()
                return [
                    {
                        "contract_id": str(r[0]),
                        "contract_number": r[1],
                        "title": r[2],
                        "contract_type": r[3],
                        "procurement_method": r[4],
                        "agency_name": r[5],
                        "nigp_codes": r[6] or [],
                        "naics_codes": r[7] or [],
                    }
                    for r in rows
                ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# CBE SUMMARY
# =============================================================================

@app.get("/api/cbe/summary")
def get_cbe_summary() -> dict[str, Any]:
    """Get CBE vs Non-CBE summary statistics."""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT
                        COUNT(*) FILTER (WHERE is_cbe) as cbe_count,
                        COUNT(*) FILTER (WHERE NOT is_cbe OR is_cbe IS NULL) as non_cbe_count
                    FROM analytics.dim_supplier
                """)
                row = cur.fetchone()

                # Get spend by CBE status
                cur.execute("""
                    SELECT
                        COALESCE(SUM(f.spend_amount) FILTER (WHERE s.is_cbe), 0) as cbe_spend,
                        COALESCE(SUM(f.spend_amount) FILTER (WHERE NOT s.is_cbe OR s.is_cbe IS NULL), 0) as non_cbe_spend
                    FROM analytics.fact_spend f
                    LEFT JOIN analytics.dim_supplier s ON f.supplier_id = s.supplier_id
                """)
                spend_row = cur.fetchone()

                return {
                    "cbeSupplierCount": row[0] or 0,
                    "nonCbeSupplierCount": row[1] or 0,
                    "cbeSpend": float(spend_row[0] or 0),
                    "nonCbeSpend": float(spend_row[1] or 0),
                }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# COMBINED DATA ENDPOINT
# =============================================================================

@app.get("/api/all")
def get_all_dashboard_data() -> dict[str, Any]:
    """Get all dashboard data in a single request."""
    try:
        return {
            "kpis": get_kpis(),
            "spendByFiscalYear": get_spend_by_fiscal_year(),
            "spendByAgency": get_spend_by_agency(),
            "topSuppliers": get_top_suppliers(),
            "cbeSummary": get_cbe_summary(),
            "exportedAt": datetime.now().isoformat(),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# ROOT HTML DASHBOARD
# =============================================================================

@app.get("/", response_class=HTMLResponse)
def root():
    """Serve a simple dashboard HTML page."""
    return """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>DC Data Platform Dashboard</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #f5f7fa; color: #333; }
        .header { background: linear-gradient(135deg, #1e3a5f 0%, #2d5a87 100%); color: white; padding: 2rem; }
        .header h1 { font-size: 1.8rem; margin-bottom: 0.5rem; }
        .header p { opacity: 0.8; }
        .container { max-width: 1400px; margin: 0 auto; padding: 2rem; }
        .kpi-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 1.5rem; margin-bottom: 2rem; }
        .kpi-card { background: white; border-radius: 12px; padding: 1.5rem; box-shadow: 0 2px 10px rgba(0,0,0,0.05); }
        .kpi-card .value { font-size: 2rem; font-weight: 700; color: #1e3a5f; }
        .kpi-card .label { color: #666; font-size: 0.9rem; margin-top: 0.5rem; }
        .chart-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(400px, 1fr)); gap: 1.5rem; }
        .chart-card { background: white; border-radius: 12px; padding: 1.5rem; box-shadow: 0 2px 10px rgba(0,0,0,0.05); }
        .chart-card h3 { margin-bottom: 1rem; color: #1e3a5f; }
        .loading { text-align: center; padding: 2rem; color: #666; }
        .error { background: #fee; color: #c00; padding: 1rem; border-radius: 8px; margin-bottom: 1rem; }
        table { width: 100%; border-collapse: collapse; margin-top: 1rem; }
        th, td { text-align: left; padding: 0.75rem; border-bottom: 1px solid #eee; }
        th { background: #f8f9fa; font-weight: 600; }
        .cbe-badge { background: #28a745; color: white; padding: 2px 8px; border-radius: 4px; font-size: 0.75rem; }
    </style>
</head>
<body>
    <div class="header">
        <h1>DC Data Platform Dashboard</h1>
        <p>Real-time procurement analytics</p>
    </div>
    <div class="container">
        <div id="error" class="error" style="display: none;"></div>
        <div class="kpi-grid" id="kpis">
            <div class="loading">Loading KPIs...</div>
        </div>
        <div class="chart-grid">
            <div class="chart-card">
                <h3>Spend by Fiscal Year (Millions $)</h3>
                <canvas id="fyChart"></canvas>
            </div>
            <div class="chart-card">
                <h3>Top Agencies by Spend (Millions $)</h3>
                <canvas id="agencyChart"></canvas>
            </div>
        </div>
        <div class="chart-card" style="margin-top: 1.5rem;">
            <h3>Top Suppliers</h3>
            <table id="suppliersTable">
                <thead>
                    <tr><th>#</th><th>Supplier</th><th>CBE</th><th>Payments</th><th>Total Spend</th></tr>
                </thead>
                <tbody id="suppliersBody">
                    <tr><td colspan="5" class="loading">Loading...</td></tr>
                </tbody>
            </table>
        </div>
    </div>
    <script>
        async function loadDashboard() {
            try {
                const resp = await fetch('/api/all');
                if (!resp.ok) throw new Error('API error: ' + resp.status);
                const data = await resp.json();

                // KPIs
                const kpis = data.kpis;
                document.getElementById('kpis').innerHTML = `
                    <div class="kpi-card"><div class="value">${kpis.totalSuppliers.toLocaleString()}</div><div class="label">Total Suppliers</div></div>
                    <div class="kpi-card"><div class="value">${kpis.cbeSuppliers.toLocaleString()}</div><div class="label">CBE Suppliers</div></div>
                    <div class="kpi-card"><div class="value">${kpis.totalContracts.toLocaleString()}</div><div class="label">Contracts</div></div>
                    <div class="kpi-card"><div class="value">${kpis.totalPayments.toLocaleString()}</div><div class="label">Payments</div></div>
                    <div class="kpi-card"><div class="value">$${(kpis.totalSpend/1e9).toFixed(2)}B</div><div class="label">Total Spend</div></div>
                    <div class="kpi-card"><div class="value">${kpis.cbeRate}%</div><div class="label">CBE Rate</div></div>
                `;

                // Fiscal Year Chart
                new Chart(document.getElementById('fyChart'), {
                    type: 'bar',
                    data: {
                        labels: data.spendByFiscalYear.labels,
                        datasets: [{ label: 'Spend ($M)', data: data.spendByFiscalYear.data, backgroundColor: '#1e3a5f' }]
                    },
                    options: { responsive: true, plugins: { legend: { display: false } } }
                });

                // Agency Chart
                new Chart(document.getElementById('agencyChart'), {
                    type: 'bar',
                    data: {
                        labels: data.spendByAgency.labels,
                        datasets: [{ label: 'Spend ($M)', data: data.spendByAgency.data, backgroundColor: '#2d5a87' }]
                    },
                    options: { responsive: true, indexAxis: 'y', plugins: { legend: { display: false } } }
                });

                // Suppliers Table
                const tbody = document.getElementById('suppliersBody');
                tbody.innerHTML = data.topSuppliers.map(s => `
                    <tr>
                        <td>${s.rank}</td>
                        <td>${s.name}</td>
                        <td>${s.is_cbe ? '<span class="cbe-badge">CBE</span>' : ''}</td>
                        <td>${s.payment_count.toLocaleString()}</td>
                        <td>$${(s.total_spend/1e6).toFixed(2)}M</td>
                    </tr>
                `).join('');

            } catch (e) {
                document.getElementById('error').textContent = 'Error loading data: ' + e.message;
                document.getElementById('error').style.display = 'block';
            }
        }
        loadDashboard();
    </script>
</body>
</html>
"""
