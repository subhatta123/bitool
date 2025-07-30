-- ConvaBI PostgreSQL Database Initialization Script
-- This script runs automatically when the PostgreSQL container starts for the first time

-- Create additional extensions if needed
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";

-- Set default encoding and locale
SET client_encoding = 'UTF8';

-- Create database user if not exists (backup in case environment variables fail)
DO $$ 
BEGIN 
    IF NOT EXISTS (SELECT 1 FROM pg_user WHERE usename = 'dbchat_user') THEN
        CREATE USER dbchat_user WITH ENCRYPTED PASSWORD 'dbchat_password';
        GRANT ALL PRIVILEGES ON DATABASE dbchat TO dbchat_user;
    END IF;
END $$;

-- Set timezone
SET timezone = 'UTC';

-- Performance optimizations for Django
ALTER SYSTEM SET shared_preload_libraries = 'pg_stat_statements';
ALTER SYSTEM SET max_connections = 200;
ALTER SYSTEM SET shared_buffers = '256MB';
ALTER SYSTEM SET effective_cache_size = '1GB';
ALTER SYSTEM SET work_mem = '4MB';
ALTER SYSTEM SET maintenance_work_mem = '64MB';
ALTER SYSTEM SET random_page_cost = 1.1;
ALTER SYSTEM SET temp_file_limit = '2GB';
ALTER SYSTEM SET log_min_duration_statement = 1000;
ALTER SYSTEM SET log_statement = 'ddl';
ALTER SYSTEM SET log_checkpoints = on;
ALTER SYSTEM SET log_connections = on;
ALTER SYSTEM SET log_disconnections = on;
ALTER SYSTEM SET log_lock_waits = on;

-- Reload configuration
SELECT pg_reload_conf();

-- Create schema for application data if needed
CREATE SCHEMA IF NOT EXISTS app_data;

-- Grant permissions
GRANT USAGE ON SCHEMA app_data TO PUBLIC;
GRANT CREATE ON SCHEMA app_data TO PUBLIC;

-- Log successful initialization
\echo 'ConvaBI PostgreSQL database initialized successfully!' 