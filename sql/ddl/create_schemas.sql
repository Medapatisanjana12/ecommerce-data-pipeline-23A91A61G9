-- Create schemas
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS production;
CREATE SCHEMA IF NOT EXISTS warehouse;

-- ======================
-- STAGING TABLES
-- ======================
CREATE TABLE IF NOT EXISTS staging.customers (
    customer_id TEXT PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    email TEXT,
    phone TEXT,
    registration_date DATE,
    city TEXT,
    state TEXT,
    country TEXT,
    age_group TEXT,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS staging.products (
    product_id TEXT PRIMARY KEY,
    product_name TEXT,
    category TEXT,
    sub_category TEXT,
    price NUMERIC,
    cost NUMERIC,
    brand TEXT,
    stock_quantity INT,
    supplier_id TEXT,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS staging.transactions (
    transaction_id TEXT PRIMARY KEY,
    customer_id TEXT,
    transaction_date DATE,
    transaction_time TEXT,
    payment_method TEXT,
    shipping_address TEXT,
    total_amount NUMERIC,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS staging.transaction_items (
    item_id TEXT PRIMARY KEY,
    transaction_id TEXT,
    product_id TEXT,
    quantity INT,
    unit_price NUMERIC,
    discount_percentage INT,
    line_total NUMERIC,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ======================
-- PRODUCTION TABLES
-- ======================
CREATE TABLE IF NOT EXISTS production.customers (
    customer_id TEXT PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    email TEXT,
    city TEXT,
    state TEXT,
    country TEXT,
    age_group TEXT
);

CREATE TABLE IF NOT EXISTS production.products (
    product_id TEXT PRIMARY KEY,
    product_name TEXT,
    category TEXT,
    sub_category TEXT,
    price NUMERIC,
    cost NUMERIC,
    brand TEXT
);

CREATE TABLE IF NOT EXISTS production.transactions (
    transaction_id TEXT PRIMARY KEY,
    customer_id TEXT,
    transaction_date DATE,
    payment_method TEXT,
    total_amount NUMERIC
);

CREATE TABLE IF NOT EXISTS production.transaction_items (
    item_id TEXT PRIMARY KEY,
    transaction_id TEXT,
    product_id TEXT,
    quantity INT,
    unit_price NUMERIC,
    line_total NUMERIC
);
