-- Semantic Layer Database Schema for ConvaBI Application
-- Stores business metadata to enhance LLM query generation

-- Table to store semantic information about database tables
CREATE TABLE semantic_tables (
    id SERIAL PRIMARY KEY,
    table_name VARCHAR(255) UNIQUE NOT NULL,
    display_name VARCHAR(255) NOT NULL,
    description TEXT,
    business_purpose TEXT,
    common_queries JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table to store semantic information about columns
CREATE TABLE semantic_columns (
    id SERIAL PRIMARY KEY,
    table_name VARCHAR(255) NOT NULL REFERENCES semantic_tables(table_name) ON DELETE CASCADE,
    column_name VARCHAR(255) NOT NULL,
    display_name VARCHAR(255) NOT NULL,
    description TEXT NOT NULL,
    semantic_type VARCHAR(50) NOT NULL CHECK (semantic_type IN ('dimension', 'measure', 'identifier', 'date')),
    data_type VARCHAR(100),
    sample_values JSONB,
    common_filters JSONB,
    business_rules JSONB,
    aggregation_default VARCHAR(50),
    is_nullable BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(table_name, column_name)
);

-- Table to store relationships between tables
CREATE TABLE semantic_relationships (
    id SERIAL PRIMARY KEY,
    from_table VARCHAR(255) NOT NULL,
    from_column VARCHAR(255) NOT NULL,
    to_table VARCHAR(255) NOT NULL,
    to_column VARCHAR(255) NOT NULL,
    relationship_type VARCHAR(50) NOT NULL CHECK (relationship_type IN ('one_to_one', 'one_to_many', 'many_to_one', 'many_to_many')),
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (from_table, from_column) REFERENCES semantic_columns(table_name, column_name),
    FOREIGN KEY (to_table, to_column) REFERENCES semantic_columns(table_name, column_name)
);

-- Table to store global business metrics and calculations
CREATE TABLE semantic_metrics (
    id SERIAL PRIMARY KEY,
    metric_name VARCHAR(255) UNIQUE NOT NULL,
    display_name VARCHAR(255) NOT NULL,
    description TEXT,
    formula TEXT NOT NULL,
    category VARCHAR(100),
    tables_involved JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table to store business glossary terms
CREATE TABLE semantic_glossary (
    id SERIAL PRIMARY KEY,
    term VARCHAR(255) UNIQUE NOT NULL,
    definition TEXT NOT NULL,
    category VARCHAR(100),
    related_tables JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for performance
CREATE INDEX idx_semantic_columns_table ON semantic_columns(table_name);
CREATE INDEX idx_semantic_relationships_from ON semantic_relationships(from_table, from_column);
CREATE INDEX idx_semantic_relationships_to ON semantic_relationships(to_table, to_column);

-- Insert sample data for demonstration
INSERT INTO semantic_tables (table_name, display_name, description, business_purpose, common_queries) VALUES
('customers', 'Customer Directory', 'Master list of all customers with their basic information', 'Track customer information and geographic assignments', 
 '["List customers by region", "Count customers", "Find customer by name"]'),
('orders', 'Sales Orders', 'Transactional data for all customer orders', 'Record sales transactions with revenue and profit data',
 '["Total sales by month", "Revenue by customer", "Profit margin analysis", "Regional sales comparison"]');

INSERT INTO semantic_columns (table_name, column_name, display_name, description, semantic_type, data_type, sample_values, common_filters, business_rules, aggregation_default) VALUES
-- Customers table columns
('customers', 'customer_id', 'Customer ID', 'Unique identifier for each customer', 'identifier', 'integer', 
 '["1001", "1002", "1003"]', '[]', '["Always positive", "Never null", "Unique across system"]', NULL),
('customers', 'customer_name', 'Customer Name', 'Full legal name of the customer', 'dimension', 'varchar', 
 '["John Smith", "ABC Corp", "Jane Doe", "XYZ Ltd"]', '["LIKE ''%Corp%''", "LIKE ''%LLC%''", "NOT LIKE ''%Test%''"]', '["Cannot be empty", "No special characters except spaces and periods"]', NULL),
('customers', 'region', 'Sales Region', 'Geographic territory assigned to customer', 'dimension', 'varchar',
 '["North", "South", "East", "West"]', '["= ''North''", "IN (''North'', ''South'')", "!= ''Test''"]', '["Must be one of: North, South, East, West", "Cannot be null"]', NULL),

-- Orders table columns  
('orders', 'order_id', 'Order ID', 'Unique identifier for each order', 'identifier', 'integer',
 '["5001", "5002", "5003"]', '[]', '["Always positive", "Never null", "Unique per order"]', NULL),
('orders', 'customer_id', 'Customer ID', 'Reference to customer who placed the order', 'identifier', 'integer',
 '["1001", "1002", "1003"]', '[]', '["Must exist in customers table", "Never null"]', NULL),
('orders', 'order_date', 'Order Date', 'Date when the order was placed', 'date', 'date',
 '["2023-01-15", "2023-02-20", "2023-03-10"]', '["YEAR = 2023", "MONTH = 12", ">= ''2023-01-01''"]', '["Cannot be future date", "Cannot be before 2020-01-01"]', 'MIN'),
('orders', 'sales_amount', 'Sales Amount', 'Total revenue generated from this order (before costs)', 'measure', 'decimal',
 '["150.00", "2500.75", "89.99", "1200.00"]', '[]', '["Always positive", "Never null", "In USD currency"]', 'SUM'),
('orders', 'profit_amount', 'Profit Amount', 'Net profit from this order (revenue minus costs)', 'measure', 'decimal',
 '["45.00", "750.25", "12.99", "300.00"]', '[]', '["Can be negative", "Never null", "In USD currency"]', 'SUM');

INSERT INTO semantic_relationships (from_table, from_column, to_table, to_column, relationship_type, description) VALUES
('customers', 'customer_id', 'orders', 'customer_id', 'one_to_many', 'Each customer can have multiple orders');

INSERT INTO semantic_metrics (metric_name, display_name, description, formula, category, tables_involved) VALUES
('total_revenue', 'Total Revenue', 'Sum of all sales amounts', 'SUM(sales_amount)', 'Financial', '["orders"]'),
('profit_margin', 'Profit Margin %', 'Percentage of profit relative to revenue', '(SUM(profit_amount) / SUM(sales_amount)) * 100', 'Financial', '["orders"]'),
('customer_count', 'Customer Count', 'Total number of unique customers', 'COUNT(DISTINCT customer_id)', 'Customer', '["customers", "orders"]'),
('avg_order_value', 'Average Order Value', 'Average sales amount per order', 'AVG(sales_amount)', 'Financial', '["orders"]');

INSERT INTO semantic_glossary (term, definition, category, related_tables) VALUES
('Customer', 'Individual or organization that purchases products from the company', 'Business Entity', '["customers", "orders"]'),
('Revenue', 'Total income generated from sales before deducting costs', 'Financial', '["orders"]'),
('Profit', 'Revenue minus costs and expenses', 'Financial', '["orders"]'),
('Region', 'Geographic sales territory used for organizing customers and sales teams', 'Geography', '["customers"]'),
('Order', 'A transaction where a customer purchases products', 'Transaction', '["orders"]'); 