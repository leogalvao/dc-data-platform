"""CLI for DC Procurement Warehouse data loading."""

from __future__ import annotations

import logging
import sys
from pathlib import Path

import typer
from rich.console import Console
from rich.logging import RichHandler
from rich.table import Table

from warehouse.config import config

app = typer.Typer(
    name="warehouse",
    help="DC Procurement Warehouse - Load data from scrapers_unified into PostgreSQL",
    add_completion=False,
)
console = Console()


def setup_logging(verbose: bool = False) -> None:
    """Configure logging with rich handler."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(message)s",
        datefmt="[%X]",
        handlers=[RichHandler(rich_tracebacks=True, console=console)],
    )


@app.command()
def load_dimensions(
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable verbose output"),
) -> None:
    """Load dimension tables from Silver Parquet data."""
    setup_logging(verbose)
    from warehouse.loaders import DimContractLoader, DimGeographyLoader, DimSupplierLoader

    console.print("\n[bold blue]Loading Dimension Tables[/bold blue]\n")

    results = []

    # Load suppliers
    with console.status("[bold green]Loading dim_supplier..."):
        loader = DimSupplierLoader()
        result = loader.run()
        results.append(("dim_supplier", result))

    # Load contracts
    with console.status("[bold green]Loading dim_contract..."):
        loader = DimContractLoader()
        result = loader.run()
        results.append(("dim_contract", result))

    # Load geography
    with console.status("[bold green]Loading dim_geography..."):
        loader = DimGeographyLoader()
        result = loader.run()
        results.append(("dim_geography", result))

    # Display results
    _display_results(results)


@app.command()
def load_facts(
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable verbose output"),
) -> None:
    """Load fact tables from Silver Parquet data."""
    setup_logging(verbose)
    from warehouse.loaders import FactSpendLoader

    console.print("\n[bold blue]Loading Fact Tables[/bold blue]\n")

    with console.status("[bold green]Loading fact_spend..."):
        loader = FactSpendLoader()
        result = loader.run()

    _display_results([("fact_spend", result)])


@app.command()
def load_gold(
    dataset: str = typer.Option(None, "--dataset", "-d", help="Specific dataset to load"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable verbose output"),
) -> None:
    """Load Gold layer Parquet into warehouse tables."""
    setup_logging(verbose)
    from warehouse.loaders import GoldTableLoader

    console.print("\n[bold blue]Loading Gold Tables[/bold blue]\n")

    loader = GoldTableLoader()

    if dataset:
        with console.status(f"[bold green]Loading {dataset}..."):
            result = loader.load_dataset(dataset)
        results = [(dataset, result)]
    else:
        results = []
        for name in loader.GOLD_MAPPINGS.keys():
            with console.status(f"[bold green]Loading {name}..."):
                result = loader.load_dataset(name)
                results.append((name, result))

    _display_results(results)


@app.command()
def load_all(
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable verbose output"),
) -> None:
    """Run full warehouse load: dimensions -> facts -> gold."""
    setup_logging(verbose)
    from warehouse.loaders import (
        DimContractLoader,
        DimGeographyLoader,
        DimSupplierLoader,
        FactSpendLoader,
        GoldTableLoader,
    )

    console.print("\n[bold blue]Full Warehouse Load[/bold blue]\n")

    all_results = []

    # 1. Dimensions
    console.print("[bold]Step 1: Loading Dimensions[/bold]")
    for name, LoaderClass in [
        ("dim_supplier", DimSupplierLoader),
        ("dim_contract", DimContractLoader),
        ("dim_geography", DimGeographyLoader),
    ]:
        with console.status(f"[bold green]Loading {name}..."):
            loader = LoaderClass()
            result = loader.run()
            all_results.append((name, result))

    # 2. Facts
    console.print("\n[bold]Step 2: Loading Facts[/bold]")
    with console.status("[bold green]Loading fact_spend..."):
        loader = FactSpendLoader()
        result = loader.run()
        all_results.append(("fact_spend", result))

    # 3. Gold
    console.print("\n[bold]Step 3: Loading Gold[/bold]")
    gold_loader = GoldTableLoader()
    for name in gold_loader.GOLD_MAPPINGS.keys():
        with console.status(f"[bold green]Loading {name}..."):
            result = gold_loader.load_dataset(name)
            all_results.append((f"gold_{name}", result))

    # Display all results
    console.print("\n[bold]Summary[/bold]")
    _display_results(all_results)


@app.command()
def status() -> None:
    """Show warehouse connection status and table counts."""
    import psycopg

    console.print("\n[bold blue]Warehouse Status[/bold blue]\n")

    # Connection info
    console.print(f"[bold]Database:[/bold] {config.warehouse_db.database}")
    console.print(f"[bold]Host:[/bold] {config.warehouse_db.host}:{config.warehouse_db.port}")
    console.print(f"[bold]Silver Path:[/bold] {config.silver_path}")
    console.print(f"[bold]Gold Path:[/bold] {config.gold_path}")

    # Test connection and get counts
    try:
        with psycopg.connect(config.warehouse_db.connection_string) as conn:
            with conn.cursor() as cursor:
                console.print("\n[green]Connection: OK[/green]\n")

                table = Table(title="Table Counts")
                table.add_column("Table", style="cyan")
                table.add_column("Rows", justify="right", style="green")

                tables = [
                    "analytics.dim_supplier",
                    "analytics.dim_contract",
                    "analytics.dim_geography",
                    "analytics.dim_date",
                    "analytics.fact_spend",
                    "analytics.gold_spend_by_vendor",
                    "analytics.gold_property_by_ward",
                    "analytics.gold_agency_spend",
                    "analytics.gold_market_trends",
                ]

                for tbl in tables:
                    try:
                        cursor.execute(f"SELECT COUNT(*) FROM {tbl}")
                        count = cursor.fetchone()[0]
                        table.add_row(tbl, f"{count:,}")
                    except Exception:
                        table.add_row(tbl, "[red]N/A[/red]")

                console.print(table)

    except Exception as e:
        console.print(f"\n[red]Connection Failed:[/red] {e}")
        sys.exit(1)


@app.command()
def init_db(
    sql_file: Path = typer.Argument(
        ...,
        help="Path to SQL file to execute",
        exists=True,
    ),
) -> None:
    """Execute SQL file against warehouse database."""
    import psycopg

    console.print(f"\n[bold blue]Executing SQL: {sql_file}[/bold blue]\n")

    sql_content = sql_file.read_text()

    try:
        with psycopg.connect(config.warehouse_db.connection_string) as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql_content)
            conn.commit()
        console.print("[green]SQL executed successfully[/green]")
    except Exception as e:
        console.print(f"[red]Error:[/red] {e}")
        sys.exit(1)


def _display_results(results: list[tuple[str, dict]]) -> None:
    """Display load results in a table."""
    table = Table()
    table.add_column("Table", style="cyan")
    table.add_column("Status", style="bold")
    table.add_column("Inserted", justify="right")
    table.add_column("Updated", justify="right")
    table.add_column("Total", justify="right")

    for name, result in results:
        status = result.get("status", "unknown")
        if status == "success":
            status_display = "[green]success[/green]"
        elif status == "skipped":
            status_display = f"[yellow]skipped ({result.get('reason', '')})[/yellow]"
        else:
            status_display = f"[red]{status}[/red]"

        table.add_row(
            name,
            status_display,
            str(result.get("inserted", "-")),
            str(result.get("updated", "-")),
            str(result.get("total", result.get("rows_loaded", "-"))),
        )

    console.print(table)


@app.command()
def export_dashboard_data(
    output: Path = typer.Option(
        None, "--output", "-o",
        help="Output path (default: dashboard/data.json)"
    ),
) -> None:
    """Export warehouse data to JSON for dashboard."""
    import json
    from datetime import datetime
    import psycopg

    console.print("[bold blue]Exporting dashboard data...[/bold blue]")

    data = {
        "exported_at": datetime.now().isoformat(),
        "kpis": {},
        "monthlySpend": {"labels": [], "data": []},
        "spendByWard": {"labels": [], "data": []},
        "agencySpend": {"labels": [], "data": []},
        "vendorTiers": {"labels": [], "data": []},
        "topVendors": [],
    }

    try:
        with psycopg.connect(config.warehouse_db.connection_string) as conn:
            with conn.cursor() as cur:
                # KPI Summary
                cur.execute("SELECT * FROM analytics.mv_kpi_summary LIMIT 1")
                row = cur.fetchone()
                if row:
                    data["kpis"] = {
                        "totalContracts": row[1] or 0,
                        "totalSpend": float(row[2] or 0),
                        "totalVendors": row[3] or 0,
                        "avgPayment": float(row[4] or 0),
                        "cbeRate": float(row[5] or 0),
                    }

                # Monthly Spend Trend
                cur.execute("""
                    SELECT year_month, total_spend
                    FROM analytics.mv_monthly_spend_trend
                    ORDER BY year_month DESC
                    LIMIT 12
                """)
                rows = cur.fetchall()[::-1]
                data["monthlySpend"] = {
                    "labels": [r[0] for r in rows],
                    "data": [round(float(r[1] or 0) / 1_000_000, 2) for r in rows]
                }

                # Spend by Ward
                cur.execute("""
                    SELECT ward, SUM(total_spend) as total
                    FROM analytics.mv_spend_by_ward
                    GROUP BY ward
                    ORDER BY ward
                """)
                rows = cur.fetchall()
                data["spendByWard"] = {
                    "labels": [r[0] for r in rows],
                    "data": [round(float(r[1] or 0) / 1_000_000, 2) for r in rows]
                }

                # Agency Spend (top 8)
                cur.execute("""
                    SELECT agency_name_normalized, total_contract_value
                    FROM analytics.gold_agency_spend
                    ORDER BY total_contract_value DESC NULLS LAST
                    LIMIT 8
                """)
                rows = cur.fetchall()
                data["agencySpend"] = {
                    "labels": [r[0][:20] if r[0] else "Unknown" for r in rows],
                    "data": [round(float(r[1] or 0) / 1_000_000, 2) for r in rows]
                }

                # Vendor Tiers
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
                data["vendorTiers"] = {
                    "labels": [r[0] for r in rows],
                    "data": [round(float(r[1] or 0) / 1_000_000, 2) for r in rows]
                }

                # Top Vendors
                cur.execute("""
                    SELECT
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
                    LIMIT 10
                """)
                rows = cur.fetchall()
                data["topVendors"] = [
                    {
                        "rank": i + 1,
                        "name": r[0] or "Unknown",
                        "normalized": r[1] or "",
                        "payments": float(r[2] or 0),
                        "contracts": r[3] or 0,
                        "agencies": r[4] or 0,
                        "isCbe": r[5] or False,
                        "tier": r[6],
                    }
                    for i, r in enumerate(rows)
                ]

                # Property count from gold_property_by_ward
                cur.execute("""
                    SELECT COALESCE(SUM(property_count), 0)
                    FROM analytics.gold_property_by_ward
                """)
                row = cur.fetchone()
                data["kpis"]["propertyCount"] = int(row[0]) if row else 0

    except Exception as e:
        console.print(f"[red]Database error:[/red] {e}")
        console.print("[yellow]Exporting with empty data...[/yellow]")

    # Determine output path
    if output is None:
        output = Path(__file__).parent / "dashboard" / "data.json"

    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(data, indent=2))
    console.print(f"[green]✓ Exported to {output}[/green]")


@app.command()
def serve(
    port: int = typer.Option(8080, "--port", "-p", help="Port to serve on"),
    reload: bool = typer.Option(False, "--reload", "-r", help="Enable auto-reload"),
) -> None:
    """Serve dashboard with live API."""
    try:
        import uvicorn
    except ImportError:
        console.print("[red]uvicorn not installed. Run: pip install 'dc-warehouse[api]'[/red]")
        sys.exit(1)

    console.print(f"\n[bold blue]DC Warehouse Dashboard[/bold blue]")
    console.print(f"[green]Dashboard:[/green] http://localhost:{port}")
    console.print(f"[green]API Docs:[/green]  http://localhost:{port}/docs")
    console.print(f"[green]Health:[/green]    http://localhost:{port}/api/health")
    console.print("\nPress Ctrl+C to stop\n")

    uvicorn.run(
        "warehouse.api:app",
        host="0.0.0.0",
        port=port,
        reload=reload,
        log_level="info",
    )


if __name__ == "__main__":
    app()
