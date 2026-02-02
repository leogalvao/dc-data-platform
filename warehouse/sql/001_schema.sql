-- DC Data Platform Warehouse Schema
-- Creates analytics schema and dimension/fact tables

-- Create analytics schema
CREATE SCHEMA IF NOT EXISTS analytics;

-- Drop existing tables to recreate with correct schema
DROP TABLE IF EXISTS analytics.fact_spend CASCADE;
DROP TABLE IF EXISTS analytics.dim_supplier CASCADE;
DROP TABLE IF EXISTS analytics.dim_contract CASCADE;
DROP TABLE IF EXISTS analytics.dim_geography CASCADE;
DROP TABLE IF EXISTS analytics.gold_spend_by_vendor CASCADE;
DROP TABLE IF EXISTS analytics.gold_spend_by_agency CASCADE;
DROP TABLE IF EXISTS analytics.gold_contracts_summary CASCADE;
DROP MATERIALIZED VIEW IF EXISTS analytics.mv_kpi_summary CASCADE;

-- Dimension: Suppliers (matches dim_supplier.py loader)
CREATE TABLE analytics.dim_supplier (
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

CREATE INDEX idx_supplier_name ON analytics.dim_supplier(normalized_name);
CREATE INDEX idx_supplier_cbe ON analytics.dim_supplier(is_cbe);
CREATE INDEX idx_supplier_supplier_name ON analytics.dim_supplier(supplier_name);

-- Dimension: Contracts (matches dim_contract.py loader)
CREATE TABLE analytics.dim_contract (
    contract_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    contract_number VARCHAR(100) NOT NULL,
    contract_title VARCHAR(1000),
    contract_type VARCHAR(100),
    procurement_method VARCHAR(100),
    nigp_codes TEXT[],
    naics_codes TEXT[],
    agency_code VARCHAR(100),
    agency_name VARCHAR(500),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_contract_number ON analytics.dim_contract(contract_number);
CREATE INDEX idx_contract_agency ON analytics.dim_contract(agency_name);

-- Dimension: Geography
CREATE TABLE analytics.dim_geography (
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

CREATE INDEX idx_geo_ward ON analytics.dim_geography(ward);
CREATE INDEX idx_geo_zip ON analytics.dim_geography(zip_code);

-- Fact: Spend (matches fact_spend.py loader)
CREATE TABLE analytics.fact_spend (
    spend_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    payment_uuid UUID,
    supplier_id UUID REFERENCES analytics.dim_supplier(supplier_id),
    contract_id UUID REFERENCES analytics.dim_contract(contract_id),
    geo_id UUID REFERENCES analytics.dim_geography(geo_id),
    fiscal_year INTEGER,
    fiscal_quarter INTEGER,
    fiscal_month INTEGER,
    payment_date DATE,
    spend_amount NUMERIC(18, 2),
    payment_type VARCHAR(100),
    agency_code VARCHAR(100),
    agency_name VARCHAR(500),
    fund_type VARCHAR(100),
    appropriation VARCHAR(200),
    cost_center VARCHAR(100),
    vendor_name VARCHAR(500),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_spend_supplier ON analytics.fact_spend(supplier_id);
CREATE INDEX idx_spend_contract ON analytics.fact_spend(contract_id);
CREATE INDEX idx_spend_date ON analytics.fact_spend(payment_date);
CREATE INDEX idx_spend_fy ON analytics.fact_spend(fiscal_year, fiscal_quarter);
CREATE INDEX idx_spend_vendor ON analytics.fact_spend(vendor_name);

-- Gold layer tables for pre-aggregated data
CREATE TABLE analytics.gold_spend_by_vendor (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    vendor_name VARCHAR(500),
    fiscal_year INTEGER,
    total_amount NUMERIC(18, 2),
    payment_count INTEGER,
    avg_payment NUMERIC(18, 2),
    is_cbe BOOLEAN,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE analytics.gold_spend_by_agency (
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

CREATE TABLE analytics.gold_contracts_summary (
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
CREATE INDEX idx_gold_vendor_fy ON analytics.gold_spend_by_vendor(fiscal_year);
CREATE INDEX idx_gold_agency_fy ON analytics.gold_spend_by_agency(fiscal_year);
CREATE INDEX idx_gold_contracts_agency ON analytics.gold_contracts_summary(agency);

-- Materialized views for KPIs
CREATE MATERIALIZED VIEW analytics.mv_kpi_summary AS
SELECT
    (SELECT COUNT(*) FROM analytics.dim_supplier) as total_suppliers,
    (SELECT COUNT(*) FROM analytics.dim_supplier WHERE is_cbe) as cbe_suppliers,
    (SELECT COUNT(*) FROM analytics.dim_contract) as total_contracts,
    NOW() as last_updated;

-- Grant permissions
GRANT USAGE ON SCHEMA analytics TO PUBLIC;
GRANT SELECT ON ALL TABLES IN SCHEMA analytics TO PUBLIC;
GRANT SELECT ON ALL MATERIALIZED VIEWS IN SCHEMA analytics TO PUBLIC;
