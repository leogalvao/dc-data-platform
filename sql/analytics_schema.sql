-- sample_sql/analytics_schema_fixed.sql
-- DC Procurement Warehouse - Analytics Schema DDL (FIXED)
-- 
-- FIXES:
--   1. Added unique indexes to mv_spend_by_ward and mv_spend_by_category
--   2. Refresh function handles empty tables gracefully
--   3. Added explicit schema qualification throughout
--
-- Run against: warehouse database (procurement_warehouse)
-- ============================================================================

-- Create analytics schema
CREATE SCHEMA IF NOT EXISTS analytics;

-- Set search path
SET search_path TO analytics, public;

-- ============================================================================
-- DIMENSION TABLES
-- ============================================================================

-- Dimension: Supplier
CREATE TABLE IF NOT EXISTS analytics.dim_supplier (
    supplier_key        SERIAL PRIMARY KEY,
    supplier_uuid       UUID NOT NULL UNIQUE,
    supplier_name       VARCHAR(500) NOT NULL,
    dba_name            VARCHAR(500),
    business_address    TEXT,
    city                VARCHAR(100),
    state               VARCHAR(2),
    zip_code            VARCHAR(20),
    is_cbe              BOOLEAN DEFAULT FALSE,
    cbe_categories      TEXT[],
    registration_date   DATE,
    effective_date      DATE NOT NULL DEFAULT CURRENT_DATE,
    expiration_date     DATE DEFAULT '9999-12-31',
    is_current          BOOLEAN DEFAULT TRUE,
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_dim_supplier_uuid ON analytics.dim_supplier(supplier_uuid);
CREATE INDEX IF NOT EXISTS idx_dim_supplier_name ON analytics.dim_supplier(supplier_name);
CREATE INDEX IF NOT EXISTS idx_dim_supplier_cbe ON analytics.dim_supplier(is_cbe) WHERE is_cbe = TRUE;

-- Dimension: Contract
CREATE TABLE IF NOT EXISTS analytics.dim_contract (
    contract_key        SERIAL PRIMARY KEY,
    contract_uuid       UUID NOT NULL UNIQUE,
    contract_number     VARCHAR(100) NOT NULL,
    contract_title      VARCHAR(1000),
    contract_type       VARCHAR(100),
    procurement_method  VARCHAR(100),
    nigp_codes          TEXT[],
    naics_codes         TEXT[],
    agency_code         VARCHAR(50),
    agency_name         VARCHAR(500),
    effective_date      DATE NOT NULL DEFAULT CURRENT_DATE,
    expiration_date     DATE DEFAULT '9999-12-31',
    is_current          BOOLEAN DEFAULT TRUE,
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_dim_contract_uuid ON analytics.dim_contract(contract_uuid);
CREATE INDEX IF NOT EXISTS idx_dim_contract_number ON analytics.dim_contract(contract_number);
CREATE INDEX IF NOT EXISTS idx_dim_contract_agency ON analytics.dim_contract(agency_code);

-- Dimension: Time (Date dimension)
CREATE TABLE IF NOT EXISTS analytics.dim_date (
    date_key            INTEGER PRIMARY KEY,
    full_date           DATE NOT NULL UNIQUE,
    year                SMALLINT NOT NULL,
    quarter             SMALLINT NOT NULL,
    month               SMALLINT NOT NULL,
    month_name          VARCHAR(20) NOT NULL,
    week_of_year        SMALLINT NOT NULL,
    day_of_month        SMALLINT NOT NULL,
    day_of_week         SMALLINT NOT NULL,
    day_name            VARCHAR(20) NOT NULL,
    is_weekend          BOOLEAN NOT NULL,
    is_holiday          BOOLEAN DEFAULT FALSE,
    fiscal_year         SMALLINT NOT NULL,
    fiscal_quarter      SMALLINT NOT NULL,
    fiscal_month        SMALLINT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_dim_date_full ON analytics.dim_date(full_date);
CREATE INDEX IF NOT EXISTS idx_dim_date_fiscal_year ON analytics.dim_date(fiscal_year);

-- Dimension: Geography (Ward/Location)
CREATE TABLE IF NOT EXISTS analytics.dim_geography (
    geography_key       SERIAL PRIMARY KEY,
    ward                VARCHAR(50),
    neighborhood        VARCHAR(200),
    council_district    VARCHAR(50),
    zip_code            VARCHAR(20),
    latitude            NUMERIC(10, 7),
    longitude           NUMERIC(10, 7),
    created_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_dim_geography_ward ON analytics.dim_geography(ward);

-- ============================================================================
-- FACT TABLES
-- ============================================================================

-- Fact: Spend (grain: one row per payment/transaction)
CREATE TABLE IF NOT EXISTS analytics.fact_spend (
    spend_key           BIGSERIAL PRIMARY KEY,
    date_key            INTEGER NOT NULL REFERENCES analytics.dim_date(date_key),
    supplier_key        INTEGER REFERENCES analytics.dim_supplier(supplier_key),
    contract_key        INTEGER REFERENCES analytics.dim_contract(contract_key),
    geography_key       INTEGER REFERENCES analytics.dim_geography(geography_key),
    payment_uuid        UUID,
    po_uuid             UUID,
    contract_uuid       UUID,
    supplier_uuid       UUID,
    spend_amount        NUMERIC(15, 2) NOT NULL,
    quantity            NUMERIC(15, 4),
    payment_type        VARCHAR(50),
    fiscal_year         SMALLINT NOT NULL,
    fiscal_quarter      SMALLINT NOT NULL,
    source_system       VARCHAR(50) DEFAULT 'ocp_payments',
    etl_batch_id        VARCHAR(100),
    loaded_at           TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fact_spend_date ON analytics.fact_spend(date_key);
CREATE INDEX IF NOT EXISTS idx_fact_spend_supplier ON analytics.fact_spend(supplier_key);
CREATE INDEX IF NOT EXISTS idx_fact_spend_contract ON analytics.fact_spend(contract_key);
CREATE INDEX IF NOT EXISTS idx_fact_spend_fiscal_year ON analytics.fact_spend(fiscal_year);

-- ============================================================================
-- MATERIALIZED VIEWS (Precomputed KPIs)
-- ============================================================================

-- Drop existing views to recreate with proper indexes
DROP MATERIALIZED VIEW IF EXISTS analytics.mv_kpi_summary CASCADE;
DROP MATERIALIZED VIEW IF EXISTS analytics.mv_spend_by_ward CASCADE;
DROP MATERIALIZED VIEW IF EXISTS analytics.mv_monthly_spend_trend CASCADE;
DROP MATERIALIZED VIEW IF EXISTS analytics.mv_spend_by_category CASCADE;
DROP MATERIALIZED VIEW IF EXISTS analytics.dim_supplier_performance CASCADE;

-- MV: KPI Summary (Dashboard headline metrics)
-- Note: This is a single-row summary, so we use a constant for the unique index
CREATE MATERIALIZED VIEW analytics.mv_kpi_summary AS
SELECT
    1 AS id,  -- Constant for unique index
    (SELECT COUNT(*) FROM analytics.dim_contract WHERE is_current = TRUE)::INTEGER AS total_contracts,
    COALESCE(SUM(f.spend_amount), 0)::NUMERIC(15,2) AS total_spend,
    COUNT(DISTINCT f.supplier_key)::INTEGER AS total_vendors,
    COALESCE(AVG(f.spend_amount), 0)::NUMERIC(15,2) AS avg_payment_amount,
    COALESCE((
        SELECT COUNT(DISTINCT CASE WHEN s2.is_cbe THEN s2.supplier_key END)::NUMERIC * 100
               / NULLIF(COUNT(DISTINCT s2.supplier_key), 0)
        FROM analytics.fact_spend f2
        JOIN analytics.dim_supplier s2 ON f2.supplier_key = s2.supplier_key
    ), 0)::NUMERIC(5,2) AS cbe_participation_rate,
    NOW() AS updated_at
FROM analytics.fact_spend f
WITH NO DATA;  -- Create empty, populate later

CREATE UNIQUE INDEX idx_mv_kpi_summary_id ON analytics.mv_kpi_summary(id);

-- MV: Spend by Ward (FIXED: added unique composite index)
CREATE MATERIALIZED VIEW analytics.mv_spend_by_ward AS
SELECT
    COALESCE(g.ward, 'Unknown') AS ward,
    f.fiscal_year,
    SUM(f.spend_amount)::NUMERIC(15,2) AS total_spend,
    COUNT(DISTINCT f.contract_key)::INTEGER AS contract_count,
    COUNT(DISTINCT f.supplier_key)::INTEGER AS vendor_count,
    AVG(f.spend_amount)::NUMERIC(15,2) AS avg_transaction,
    NOW() AS updated_at
FROM analytics.fact_spend f
LEFT JOIN analytics.dim_geography g ON f.geography_key = g.geography_key
GROUP BY COALESCE(g.ward, 'Unknown'), f.fiscal_year
WITH NO DATA;

-- FIXED: Unique composite index for CONCURRENTLY refresh
CREATE UNIQUE INDEX idx_mv_spend_ward_unique ON analytics.mv_spend_by_ward(ward, fiscal_year);
CREATE INDEX idx_mv_spend_ward ON analytics.mv_spend_by_ward(ward);
CREATE INDEX idx_mv_spend_ward_fy ON analytics.mv_spend_by_ward(fiscal_year);

-- MV: Monthly Spend Trend
CREATE MATERIALIZED VIEW analytics.mv_monthly_spend_trend AS
SELECT
    TO_CHAR(d.full_date, 'YYYY-MM') AS year_month,
    d.fiscal_year,
    d.fiscal_quarter,
    SUM(f.spend_amount)::NUMERIC(15,2) AS total_spend,
    COUNT(DISTINCT f.contract_key)::INTEGER AS contract_count,
    COUNT(f.spend_key)::INTEGER AS payment_count,
    SUM(SUM(f.spend_amount)) OVER (
        PARTITION BY d.fiscal_year 
        ORDER BY TO_CHAR(d.full_date, 'YYYY-MM')
    )::NUMERIC(15,2) AS running_total,
    NOW() AS updated_at
FROM analytics.fact_spend f
JOIN analytics.dim_date d ON f.date_key = d.date_key
GROUP BY TO_CHAR(d.full_date, 'YYYY-MM'), d.fiscal_year, d.fiscal_quarter
ORDER BY year_month
WITH NO DATA;

CREATE UNIQUE INDEX idx_mv_monthly_trend ON analytics.mv_monthly_spend_trend(year_month);

-- MV: Spend by Category (FIXED: using LATERAL for unnest)
CREATE MATERIALIZED VIEW analytics.mv_spend_by_category AS
SELECT
    COALESCE(cat.code, 'Uncategorized') AS category,
    f.fiscal_year,
    SUM(f.spend_amount)::NUMERIC(15,2) AS total_spend,
    COUNT(DISTINCT f.contract_key)::INTEGER AS contract_count,
    (SUM(f.spend_amount) / NULLIF(SUM(SUM(f.spend_amount)) OVER (PARTITION BY f.fiscal_year), 0) * 100)::NUMERIC(5,2) AS pct_of_total,
    NOW() AS updated_at
FROM analytics.fact_spend f
LEFT JOIN analytics.dim_contract c ON f.contract_key = c.contract_key
LEFT JOIN LATERAL unnest(c.nigp_codes) AS cat(code) ON TRUE
GROUP BY COALESCE(cat.code, 'Uncategorized'), f.fiscal_year
WITH NO DATA;

-- FIXED: Unique composite index for CONCURRENTLY refresh
CREATE UNIQUE INDEX idx_mv_category_unique ON analytics.mv_spend_by_category(category, fiscal_year);
CREATE INDEX idx_mv_category ON analytics.mv_spend_by_category(category);

-- MV: Supplier Performance
CREATE MATERIALIZED VIEW analytics.dim_supplier_performance AS
SELECT
    s.supplier_uuid,
    s.supplier_name,
    s.is_cbe,
    COUNT(DISTINCT f.contract_key)::INTEGER AS total_contracts,
    SUM(f.spend_amount)::NUMERIC(15,2) AS total_spend,
    AVG(f.spend_amount)::NUMERIC(15,2) AS avg_payment,
    MIN(d.full_date) AS first_payment_date,
    MAX(d.full_date) AS last_payment_date,
    CASE
        WHEN SUM(f.spend_amount) >= 10000000 THEN 'Enterprise'
        WHEN SUM(f.spend_amount) >= 1000000 THEN 'Large'
        WHEN SUM(f.spend_amount) >= 100000 THEN 'Medium'
        ELSE 'Small'
    END AS performance_tier,
    NOW() AS updated_at
FROM analytics.dim_supplier s
JOIN analytics.fact_spend f ON s.supplier_key = f.supplier_key
JOIN analytics.dim_date d ON f.date_key = d.date_key
WHERE s.is_current = TRUE
GROUP BY s.supplier_uuid, s.supplier_name, s.is_cbe
WITH NO DATA;

CREATE UNIQUE INDEX idx_supplier_perf_uuid ON analytics.dim_supplier_performance(supplier_uuid);

-- ============================================================================
-- REFRESH FUNCTIONS (FIXED: Handle empty views gracefully)
-- ============================================================================

-- Function: Refresh all materialized views
-- Uses non-concurrent refresh for empty views, concurrent for populated views
CREATE OR REPLACE FUNCTION analytics.refresh_all_materialized_views()
RETURNS void AS $$
DECLARE
    view_record RECORD;
    row_count INTEGER;
BEGIN
    -- List of materialized views to refresh
    FOR view_record IN 
        SELECT matviewname 
        FROM pg_matviews 
        WHERE schemaname = 'analytics'
        ORDER BY matviewname
    LOOP
        -- Check if view has data (for CONCURRENTLY requirement)
        EXECUTE format('SELECT COUNT(*) FROM analytics.%I', view_record.matviewname) INTO row_count;
        
        IF row_count > 0 THEN
            -- Use CONCURRENTLY for populated views (no lock)
            BEGIN
                EXECUTE format('REFRESH MATERIALIZED VIEW CONCURRENTLY analytics.%I', view_record.matviewname);
                RAISE NOTICE 'Refreshed CONCURRENTLY: analytics.%', view_record.matviewname;
            EXCEPTION WHEN OTHERS THEN
                -- Fall back to regular refresh if CONCURRENTLY fails
                EXECUTE format('REFRESH MATERIALIZED VIEW analytics.%I', view_record.matviewname);
                RAISE NOTICE 'Refreshed (fallback): analytics.%', view_record.matviewname;
            END;
        ELSE
            -- Use regular refresh for empty views
            EXECUTE format('REFRESH MATERIALIZED VIEW analytics.%I', view_record.matviewname);
            RAISE NOTICE 'Refreshed (initial): analytics.%', view_record.matviewname;
        END IF;
    END LOOP;
    
    RAISE NOTICE 'All materialized views refreshed at %', NOW();
END;
$$ LANGUAGE plpgsql;

-- Function: Refresh specific view (handles empty views)
CREATE OR REPLACE FUNCTION analytics.refresh_materialized_view(view_name TEXT)
RETURNS void AS $$
DECLARE
    row_count INTEGER;
BEGIN
    -- Check if view has data
    EXECUTE format('SELECT COUNT(*) FROM analytics.%I', view_name) INTO row_count;
    
    IF row_count > 0 THEN
        BEGIN
            EXECUTE format('REFRESH MATERIALIZED VIEW CONCURRENTLY analytics.%I', view_name);
            RAISE NOTICE 'Refreshed CONCURRENTLY: analytics.%', view_name;
        EXCEPTION WHEN OTHERS THEN
            EXECUTE format('REFRESH MATERIALIZED VIEW analytics.%I', view_name);
            RAISE NOTICE 'Refreshed (fallback): analytics.%', view_name;
        END;
    ELSE
        EXECUTE format('REFRESH MATERIALIZED VIEW analytics.%I', view_name);
        RAISE NOTICE 'Refreshed (initial): analytics.%', view_name;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- HELPER: Populate Date Dimension
-- ============================================================================

CREATE OR REPLACE FUNCTION analytics.populate_dim_date(start_date DATE, end_date DATE)
RETURNS void AS $$
DECLARE
    curr_date DATE := start_date;
BEGIN
    WHILE curr_date <= end_date LOOP
        INSERT INTO analytics.dim_date (
            date_key,
            full_date,
            year,
            quarter,
            month,
            month_name,
            week_of_year,
            day_of_month,
            day_of_week,
            day_name,
            is_weekend,
            fiscal_year,
            fiscal_quarter,
            fiscal_month
        )
        VALUES (
            TO_CHAR(curr_date, 'YYYYMMDD')::INTEGER,
            curr_date,
            EXTRACT(YEAR FROM curr_date)::SMALLINT,
            EXTRACT(QUARTER FROM curr_date)::SMALLINT,
            EXTRACT(MONTH FROM curr_date)::SMALLINT,
            TO_CHAR(curr_date, 'Month'),
            EXTRACT(WEEK FROM curr_date)::SMALLINT,
            EXTRACT(DAY FROM curr_date)::SMALLINT,
            EXTRACT(ISODOW FROM curr_date)::SMALLINT,
            TO_CHAR(curr_date, 'Day'),
            EXTRACT(ISODOW FROM curr_date) IN (6, 7),
            CASE WHEN EXTRACT(MONTH FROM curr_date) >= 10 
                 THEN EXTRACT(YEAR FROM curr_date)::SMALLINT + 1
                 ELSE EXTRACT(YEAR FROM curr_date)::SMALLINT
            END,
            CASE 
                WHEN EXTRACT(MONTH FROM curr_date) IN (10, 11, 12) THEN 1
                WHEN EXTRACT(MONTH FROM curr_date) IN (1, 2, 3) THEN 2
                WHEN EXTRACT(MONTH FROM curr_date) IN (4, 5, 6) THEN 3
                ELSE 4
            END::SMALLINT,
            CASE 
                WHEN EXTRACT(MONTH FROM curr_date) >= 10 
                THEN EXTRACT(MONTH FROM curr_date)::SMALLINT - 9
                ELSE EXTRACT(MONTH FROM curr_date)::SMALLINT + 3
            END
        )
        ON CONFLICT (date_key) DO NOTHING;
        
        curr_date := curr_date + INTERVAL '1 day';
    END LOOP;
    
    RAISE NOTICE 'Populated dim_date from % to %', start_date, end_date;
END;
$$ LANGUAGE plpgsql;

-- Populate date dimension (10 years back, 2 years forward)
SELECT analytics.populate_dim_date(
    (CURRENT_DATE - INTERVAL '10 years')::DATE,
    (CURRENT_DATE + INTERVAL '2 years')::DATE
);

-- ============================================================================
-- INITIAL DATA REFRESH (populate empty materialized views)
-- ============================================================================

-- Refresh all views (non-concurrent since they're empty)
REFRESH MATERIALIZED VIEW analytics.mv_kpi_summary;
REFRESH MATERIALIZED VIEW analytics.mv_spend_by_ward;
REFRESH MATERIALIZED VIEW analytics.mv_monthly_spend_trend;
REFRESH MATERIALIZED VIEW analytics.mv_spend_by_category;
REFRESH MATERIALIZED VIEW analytics.dim_supplier_performance;

-- ============================================================================
-- GRANTS
-- ============================================================================

GRANT USAGE ON SCHEMA analytics TO warehouse_user;
GRANT SELECT ON ALL TABLES IN SCHEMA analytics TO warehouse_user;
GRANT SELECT ON ALL SEQUENCES IN SCHEMA analytics TO warehouse_user;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA analytics TO warehouse_user;

ALTER DEFAULT PRIVILEGES IN SCHEMA analytics 
    GRANT SELECT ON TABLES TO warehouse_user;

-- ============================================================================
-- VERIFICATION
-- ============================================================================

DO $$
DECLARE
    missing_views TEXT[];
    v TEXT;
BEGIN
    -- Check all expected views exist
    SELECT ARRAY_AGG(expected_view)
    INTO missing_views
    FROM (
        SELECT unnest(ARRAY[
            'mv_kpi_summary',
            'mv_spend_by_ward', 
            'mv_monthly_spend_trend',
            'mv_spend_by_category',
            'dim_supplier_performance'
        ]) AS expected_view
    ) e
    WHERE NOT EXISTS (
        SELECT 1 FROM pg_matviews 
        WHERE schemaname = 'analytics' 
        AND matviewname = e.expected_view
    );
    
    IF missing_views IS NOT NULL AND array_length(missing_views, 1) > 0 THEN
        RAISE EXCEPTION 'Missing materialized views: %', missing_views;
    ELSE
        RAISE NOTICE 'SUCCESS: All analytics materialized views created';
    END IF;
    
    -- Check function exists
    IF NOT EXISTS (
        SELECT 1 FROM pg_proc p
        JOIN pg_namespace n ON p.pronamespace = n.oid
        WHERE n.nspname = 'analytics' 
        AND p.proname = 'refresh_all_materialized_views'
    ) THEN
        RAISE EXCEPTION 'Missing function: analytics.refresh_all_materialized_views()';
    ELSE
        RAISE NOTICE 'SUCCESS: Refresh function created';
    END IF;
END $$;

-- Show summary
SELECT 
    schemaname,
    matviewname,
    pg_size_pretty(pg_total_relation_size(schemaname || '.' || matviewname)) as size,
    CASE WHEN ispopulated THEN 'Yes' ELSE 'No' END as populated
FROM pg_matviews 
WHERE schemaname = 'analytics'
ORDER BY matviewname;