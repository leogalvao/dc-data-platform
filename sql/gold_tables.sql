-- sample_sql/gold_tables.sql
-- Gold layer tables for direct Parquet load from scrapers_unified
-- Run against: warehouse database (procurement_warehouse)
-- ============================================================================

SET search_path TO analytics, public;

-- ============================================================================
-- GOLD TABLES (Pre-aggregated from scrapers_unified)
-- ============================================================================

-- Gold: Spend by Vendor (from scrapers_unified/gold/spend_by_vendor)
CREATE TABLE IF NOT EXISTS analytics.gold_spend_by_vendor (
    vendor_id VARCHAR(64) PRIMARY KEY,
    vendor_name VARCHAR(500),
    vendor_name_normalized VARCHAR(500),
    total_contract_value NUMERIC(18,2),
    total_po_value NUMERIC(18,2),
    total_payments NUMERIC(18,2),
    contract_count INTEGER,
    po_count INTEGER,
    payment_count INTEGER,
    agencies TEXT[],
    agency_count INTEGER,
    first_contract_date DATE,
    last_payment_date DATE,
    fiscal_years INTEGER[],
    is_cbe BOOLEAN,
    computed_at TIMESTAMPTZ,
    loaded_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_gold_vendor_payments
    ON analytics.gold_spend_by_vendor(total_payments DESC);
CREATE INDEX IF NOT EXISTS idx_gold_vendor_cbe
    ON analytics.gold_spend_by_vendor(is_cbe) WHERE is_cbe = TRUE;
CREATE INDEX IF NOT EXISTS idx_gold_vendor_name_norm
    ON analytics.gold_spend_by_vendor(vendor_name_normalized);


-- Gold: Property Values by Ward
CREATE TABLE IF NOT EXISTS analytics.gold_property_by_ward (
    ward VARCHAR(10) PRIMARY KEY,
    property_count INTEGER,
    total_assessed_value NUMERIC(18,2),
    avg_assessed_value NUMERIC(18,2),
    median_assessed_value NUMERIC(18,2),
    min_assessed_value NUMERIC(18,2),
    max_assessed_value NUMERIC(18,2),
    avg_sqft NUMERIC(12,2),
    avg_price_per_sqft NUMERIC(12,2),
    avg_year_built NUMERIC(6,1),
    property_type_distribution JSONB,
    bedroom_distribution JSONB,
    computed_at TIMESTAMPTZ,
    loaded_at TIMESTAMPTZ DEFAULT NOW()
);


-- Gold: Agency Spend Summary
CREATE TABLE IF NOT EXISTS analytics.gold_agency_spend (
    agency_name_normalized VARCHAR(500) PRIMARY KEY,
    agency_name VARCHAR(500),
    total_contract_value NUMERIC(18,2),
    total_payments NUMERIC(18,2),
    total_po_value NUMERIC(18,2),
    contract_count INTEGER,
    vendor_count INTEGER,
    top_vendors JSONB,
    fiscal_year_breakdown JSONB,
    computed_at TIMESTAMPTZ,
    loaded_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_gold_agency_payments
    ON analytics.gold_agency_spend(total_payments DESC);


-- Gold: Market Trends
CREATE TABLE IF NOT EXISTS analytics.gold_market_trends (
    id SERIAL PRIMARY KEY,
    region_name VARCHAR(255),
    region_type VARCHAR(50),
    metric_name VARCHAR(100),
    metric_category VARCHAR(50),
    latest_value NUMERIC(18,2),
    latest_date DATE,
    yoy_change NUMERIC(10,4),
    mom_change NUMERIC(10,4),
    trend_direction VARCHAR(20),
    historical_values JSONB,
    computed_at TIMESTAMPTZ,
    loaded_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(region_name, region_type, metric_name)
);

CREATE INDEX IF NOT EXISTS idx_gold_trends_region
    ON analytics.gold_market_trends(region_name, region_type);
CREATE INDEX IF NOT EXISTS idx_gold_trends_metric
    ON analytics.gold_market_trends(metric_name);


-- ============================================================================
-- GRANTS
-- ============================================================================

GRANT SELECT ON analytics.gold_spend_by_vendor TO warehouse_user;
GRANT SELECT ON analytics.gold_property_by_ward TO warehouse_user;
GRANT SELECT ON analytics.gold_agency_spend TO warehouse_user;
GRANT SELECT ON analytics.gold_market_trends TO warehouse_user;


-- ============================================================================
-- COMMENTS
-- ============================================================================

COMMENT ON TABLE analytics.gold_spend_by_vendor IS
    'Pre-aggregated vendor spend from scrapers_unified Gold layer';
COMMENT ON TABLE analytics.gold_property_by_ward IS
    'Property value statistics by DC ward from Gold layer';
COMMENT ON TABLE analytics.gold_agency_spend IS
    'Agency spending summary from Gold layer';
COMMENT ON TABLE analytics.gold_market_trends IS
    'Market trend data (Zillow, RentCast) from Gold layer';


-- ============================================================================
-- VERIFICATION
-- ============================================================================

DO $$
DECLARE
    missing_tables TEXT[];
BEGIN
    -- Check all expected tables exist
    SELECT ARRAY_AGG(expected_table)
    INTO missing_tables
    FROM (
        SELECT unnest(ARRAY[
            'gold_spend_by_vendor',
            'gold_property_by_ward',
            'gold_agency_spend',
            'gold_market_trends'
        ]) AS expected_table
    ) e
    WHERE NOT EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'analytics'
        AND table_name = e.expected_table
    );

    IF missing_tables IS NOT NULL AND array_length(missing_tables, 1) > 0 THEN
        RAISE EXCEPTION 'Missing gold tables: %', missing_tables;
    ELSE
        RAISE NOTICE 'SUCCESS: All gold tables created';
    END IF;
END $$;

-- Show summary
SELECT
    table_name,
    pg_size_pretty(pg_total_relation_size('analytics.' || table_name)) as size
FROM information_schema.tables
WHERE table_schema = 'analytics'
AND table_name LIKE 'gold_%'
ORDER BY table_name;
