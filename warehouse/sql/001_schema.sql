-- DC Data Platform Warehouse Schema
-- Creates analytics schema and dimension/fact tables

-- Create analytics schema
CREATE SCHEMA IF NOT EXISTS analytics;

-- Dimension: Suppliers
CREATE TABLE IF NOT EXISTS analytics.dim_supplier (
    supplier_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    supplier_name VARCHAR(500) NOT NULL,
    dba_name VARCHAR(500),
    normalized_name VARCHAR(500),
    business_address VARCHAR(500),
    city VARCHAR(100),
    state VARCHAR(50),
    zip_code VARCHAR(20),
    ward VARCHAR(10),
    is_cbe BOOLEAN DEFAULT FALSE,
    cbe_categories TEXT[],
    registration_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_supplier_name ON analytics.dim_supplier(normalized_name);
CREATE INDEX IF NOT EXISTS idx_supplier_cbe ON analytics.dim_supplier(is_cbe);

-- Dimension: Contracts
CREATE TABLE IF NOT EXISTS analytics.dim_contract (
    contract_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    contract_number VARCHAR(100) NOT NULL,
    title VARCHAR(1000),
    description TEXT,
    contract_type VARCHAR(100),
    procurement_type VARCHAR(100),
    agency VARCHAR(500),
    start_date DATE,
    end_date DATE,
    total_value NUMERIC(18, 2),
    status VARCHAR(50),
    nigp_code VARCHAR(50),
    nigp_description VARCHAR(500),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_contract_number ON analytics.dim_contract(contract_number);
CREATE INDEX IF NOT EXISTS idx_contract_agency ON analytics.dim_contract(agency);
CREATE INDEX IF NOT EXISTS idx_contract_dates ON analytics.dim_contract(start_date, end_date);

-- Dimension: Geography
CREATE TABLE IF NOT EXISTS analytics.dim_geography (
    geo_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    ward VARCHAR(10),
    neighborhood VARCHAR(100),
    zip_code VARCHAR(20),
    quadrant VARCHAR(10),
    council_district VARCHAR(10),
    anc VARCHAR(20),
    census_tract VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_geo_ward ON analytics.dim_geography(ward);
CREATE INDEX IF NOT EXISTS idx_geo_zip ON analytics.dim_geography(zip_code);

-- Fact: Spend
CREATE TABLE IF NOT EXISTS analytics.fact_spend (
    spend_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    supplier_id UUID REFERENCES analytics.dim_supplier(supplier_id),
    contract_id UUID REFERENCES analytics.dim_contract(contract_id),
    geo_id UUID REFERENCES analytics.dim_geography(geo_id),
    fiscal_year INTEGER,
    fiscal_quarter INTEGER,
    fiscal_month INTEGER,
    payment_date DATE,
    amount NUMERIC(18, 2),
    payment_type VARCHAR(100),
    agency VARCHAR(500),
    fund_type VARCHAR(100),
    appropriation VARCHAR(100),
    cost_center VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_spend_supplier ON analytics.fact_spend(supplier_id);
CREATE INDEX IF NOT EXISTS idx_spend_contract ON analytics.fact_spend(contract_id);
CREATE INDEX IF NOT EXISTS idx_spend_date ON analytics.fact_spend(payment_date);
CREATE INDEX IF NOT EXISTS idx_spend_fy ON analytics.fact_spend(fiscal_year, fiscal_quarter);

-- Gold layer tables for pre-aggregated data
CREATE TABLE IF NOT EXISTS analytics.gold_spend_by_vendor (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    vendor_name VARCHAR(500),
    fiscal_year INTEGER,
    total_amount NUMERIC(18, 2),
    payment_count INTEGER,
    avg_payment NUMERIC(18, 2),
    is_cbe BOOLEAN,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS analytics.gold_spend_by_agency (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    agency VARCHAR(500),
    fiscal_year INTEGER,
    total_amount NUMERIC(18, 2),
    vendor_count INTEGER,
    contract_count INTEGER,
    cbe_amount NUMERIC(18, 2),
    cbe_percentage NUMERIC(5, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS analytics.gold_contracts_summary (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    contract_number VARCHAR(100),
    title VARCHAR(1000),
    agency VARCHAR(500),
    vendor_name VARCHAR(500),
    total_value NUMERIC(18, 2),
    total_spent NUMERIC(18, 2),
    remaining_value NUMERIC(18, 2),
    start_date DATE,
    end_date DATE,
    status VARCHAR(50),
    is_cbe BOOLEAN,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for gold tables
CREATE INDEX IF NOT EXISTS idx_gold_vendor_fy ON analytics.gold_spend_by_vendor(fiscal_year);
CREATE INDEX IF NOT EXISTS idx_gold_agency_fy ON analytics.gold_spend_by_agency(fiscal_year);
CREATE INDEX IF NOT EXISTS idx_gold_contracts_agency ON analytics.gold_contracts_summary(agency);

-- Materialized views for KPIs
CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_kpi_summary AS
SELECT
    (SELECT COUNT(*) FROM analytics.dim_supplier) as total_suppliers,
    (SELECT COUNT(*) FROM analytics.dim_supplier WHERE is_cbe) as cbe_suppliers,
    (SELECT COUNT(*) FROM analytics.dim_contract) as total_contracts,
    (SELECT COALESCE(SUM(total_value), 0) FROM analytics.dim_contract) as total_contract_value,
    NOW() as last_updated;

-- Grant permissions
GRANT USAGE ON SCHEMA analytics TO PUBLIC;
GRANT SELECT ON ALL TABLES IN SCHEMA analytics TO PUBLIC;
GRANT SELECT ON ALL MATERIALIZED VIEWS IN SCHEMA analytics TO PUBLIC;
