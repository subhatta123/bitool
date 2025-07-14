-- =============================================================================
-- Django DBChat - PostgreSQL Initialization Script
-- =============================================================================

-- Create database if it doesn't exist (this is handled by Docker)
-- The database is already created by the POSTGRES_DB environment variable

-- Create extensions for better performance and functionality
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_stat_statements";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";

-- Create custom schemas for organization
CREATE SCHEMA IF NOT EXISTS analytics;
CREATE SCHEMA IF NOT EXISTS monitoring;
CREATE SCHEMA IF NOT EXISTS staging;

-- Create custom types
CREATE TYPE IF NOT EXISTS query_status AS ENUM ('pending', 'running', 'completed', 'failed', 'cancelled');
CREATE TYPE IF NOT EXISTS data_source_type AS ENUM ('csv', 'json', 'database', 'api', 'excel');

-- Create monitoring tables
CREATE TABLE IF NOT EXISTS monitoring.system_metrics (
    id SERIAL PRIMARY KEY,
    metric_name VARCHAR(100) NOT NULL,
    metric_value NUMERIC,
    metric_unit VARCHAR(20),
    collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    tags JSONB DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS monitoring.query_performance (
    id SERIAL PRIMARY KEY,
    query_hash VARCHAR(64) NOT NULL,
    query_text TEXT,
    execution_time_ms NUMERIC,
    rows_returned INTEGER,
    user_id INTEGER,
    database_name VARCHAR(100),
    executed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status query_status DEFAULT 'completed'
);

CREATE TABLE IF NOT EXISTS monitoring.error_logs (
    id SERIAL PRIMARY KEY,
    error_type VARCHAR(100),
    error_message TEXT,
    stack_trace TEXT,
    user_id INTEGER,
    request_path VARCHAR(500),
    request_method VARCHAR(10),
    user_agent TEXT,
    ip_address INET,
    occurred_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    resolved BOOLEAN DEFAULT FALSE
);

-- Create analytics tables
CREATE TABLE IF NOT EXISTS analytics.user_activity (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL,
    activity_type VARCHAR(50) NOT NULL,
    details JSONB DEFAULT '{}',
    session_id VARCHAR(100),
    ip_address INET,
    user_agent TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS analytics.data_usage (
    id SERIAL PRIMARY KEY,
    user_id INTEGER,
    data_source_type data_source_type,
    file_size_bytes BIGINT,
    rows_processed INTEGER,
    processing_time_ms NUMERIC,
    success BOOLEAN DEFAULT TRUE,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create staging tables for data processing
CREATE TABLE IF NOT EXISTS staging.csv_uploads (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL,
    original_filename VARCHAR(255),
    file_path VARCHAR(500),
    file_size_bytes BIGINT,
    rows_count INTEGER,
    columns_count INTEGER,
    upload_status VARCHAR(20) DEFAULT 'pending',
    processing_started_at TIMESTAMP,
    processing_completed_at TIMESTAMP,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_system_metrics_name_time ON monitoring.system_metrics(metric_name, collected_at);
CREATE INDEX IF NOT EXISTS idx_query_performance_hash ON monitoring.query_performance(query_hash);
CREATE INDEX IF NOT EXISTS idx_query_performance_user_time ON monitoring.query_performance(user_id, executed_at);
CREATE INDEX IF NOT EXISTS idx_error_logs_type_time ON monitoring.error_logs(error_type, occurred_at);
CREATE INDEX IF NOT EXISTS idx_error_logs_resolved ON monitoring.error_logs(resolved) WHERE NOT resolved;
CREATE INDEX IF NOT EXISTS idx_user_activity_user_time ON analytics.user_activity(user_id, created_at);
CREATE INDEX IF NOT EXISTS idx_user_activity_type ON analytics.user_activity(activity_type);
CREATE INDEX IF NOT EXISTS idx_data_usage_user_time ON analytics.data_usage(user_id, created_at);
CREATE INDEX IF NOT EXISTS idx_csv_uploads_user_status ON staging.csv_uploads(user_id, upload_status);

-- Create functions for common operations
CREATE OR REPLACE FUNCTION monitoring.log_query_performance(
    p_query_hash VARCHAR(64),
    p_query_text TEXT,
    p_execution_time_ms NUMERIC,
    p_rows_returned INTEGER,
    p_user_id INTEGER DEFAULT NULL,
    p_database_name VARCHAR(100) DEFAULT 'default'
) RETURNS void AS $$
BEGIN
    INSERT INTO monitoring.query_performance (
        query_hash, query_text, execution_time_ms, rows_returned, user_id, database_name
    ) VALUES (
        p_query_hash, p_query_text, p_execution_time_ms, p_rows_returned, p_user_id, p_database_name
    );
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION monitoring.log_error(
    p_error_type VARCHAR(100),
    p_error_message TEXT,
    p_stack_trace TEXT DEFAULT NULL,
    p_user_id INTEGER DEFAULT NULL,
    p_request_path VARCHAR(500) DEFAULT NULL,
    p_request_method VARCHAR(10) DEFAULT NULL,
    p_user_agent TEXT DEFAULT NULL,
    p_ip_address INET DEFAULT NULL
) RETURNS void AS $$
BEGIN
    INSERT INTO monitoring.error_logs (
        error_type, error_message, stack_trace, user_id, request_path, 
        request_method, user_agent, ip_address
    ) VALUES (
        p_error_type, p_error_message, p_stack_trace, p_user_id, p_request_path,
        p_request_method, p_user_agent, p_ip_address
    );
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION analytics.log_user_activity(
    p_user_id INTEGER,
    p_activity_type VARCHAR(50),
    p_details JSONB DEFAULT '{}',
    p_session_id VARCHAR(100) DEFAULT NULL,
    p_ip_address INET DEFAULT NULL,
    p_user_agent TEXT DEFAULT NULL
) RETURNS void AS $$
BEGIN
    INSERT INTO analytics.user_activity (
        user_id, activity_type, details, session_id, ip_address, user_agent
    ) VALUES (
        p_user_id, p_activity_type, p_details, p_session_id, p_ip_address, p_user_agent
    );
END;
$$ LANGUAGE plpgsql;

-- Create views for common queries
CREATE OR REPLACE VIEW monitoring.recent_errors AS
SELECT 
    error_type,
    error_message,
    user_id,
    request_path,
    occurred_at,
    resolved
FROM monitoring.error_logs
WHERE occurred_at > CURRENT_TIMESTAMP - INTERVAL '24 hours'
ORDER BY occurred_at DESC;

CREATE OR REPLACE VIEW monitoring.query_performance_summary AS
SELECT 
    DATE(executed_at) as date,
    database_name,
    COUNT(*) as total_queries,
    AVG(execution_time_ms) as avg_execution_time,
    MAX(execution_time_ms) as max_execution_time,
    SUM(rows_returned) as total_rows_returned
FROM monitoring.query_performance
WHERE executed_at > CURRENT_TIMESTAMP - INTERVAL '7 days'
GROUP BY DATE(executed_at), database_name
ORDER BY date DESC;

CREATE OR REPLACE VIEW analytics.user_activity_summary AS
SELECT 
    user_id,
    DATE(created_at) as date,
    activity_type,
    COUNT(*) as activity_count,
    COUNT(DISTINCT session_id) as unique_sessions
FROM analytics.user_activity
WHERE created_at > CURRENT_TIMESTAMP - INTERVAL '30 days'
GROUP BY user_id, DATE(created_at), activity_type
ORDER BY date DESC, user_id;

-- Create triggers for automatic cleanup
CREATE OR REPLACE FUNCTION cleanup_old_monitoring_data() RETURNS void AS $$
BEGIN
    -- Delete system metrics older than 30 days
    DELETE FROM monitoring.system_metrics 
    WHERE collected_at < CURRENT_TIMESTAMP - INTERVAL '30 days';
    
    -- Delete query performance data older than 90 days
    DELETE FROM monitoring.query_performance 
    WHERE executed_at < CURRENT_TIMESTAMP - INTERVAL '90 days';
    
    -- Delete resolved error logs older than 30 days
    DELETE FROM monitoring.error_logs 
    WHERE resolved = TRUE AND occurred_at < CURRENT_TIMESTAMP - INTERVAL '30 days';
    
    -- Delete user activity older than 180 days
    DELETE FROM analytics.user_activity 
    WHERE created_at < CURRENT_TIMESTAMP - INTERVAL '180 days';
    
    -- Delete old staging data
    DELETE FROM staging.csv_uploads 
    WHERE created_at < CURRENT_TIMESTAMP - INTERVAL '7 days'
    AND upload_status IN ('completed', 'failed');
END;
$$ LANGUAGE plpgsql;

-- Grant permissions
GRANT USAGE ON SCHEMA monitoring TO PUBLIC;
GRANT USAGE ON SCHEMA analytics TO PUBLIC;
GRANT USAGE ON SCHEMA staging TO PUBLIC;

GRANT SELECT ON ALL TABLES IN SCHEMA monitoring TO PUBLIC;
GRANT SELECT ON ALL TABLES IN SCHEMA analytics TO PUBLIC;
GRANT SELECT ON ALL TABLES IN SCHEMA staging TO PUBLIC;

GRANT INSERT ON monitoring.system_metrics TO PUBLIC;
GRANT INSERT ON monitoring.query_performance TO PUBLIC;
GRANT INSERT ON monitoring.error_logs TO PUBLIC;
GRANT INSERT ON analytics.user_activity TO PUBLIC;
GRANT INSERT ON analytics.data_usage TO PUBLIC;
GRANT INSERT ON staging.csv_uploads TO PUBLIC;

GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA monitoring TO PUBLIC;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA analytics TO PUBLIC;

-- Insert initial system information
INSERT INTO monitoring.system_metrics (metric_name, metric_value, metric_unit, tags) VALUES
('database_initialized', 1, 'boolean', '{"component": "postgresql", "version": "15"}'),
('schemas_created', 3, 'count', '{"schemas": ["monitoring", "analytics", "staging"]}'),
('extensions_loaded', 3, 'count', '{"extensions": ["uuid-ossp", "pg_stat_statements", "pg_trgm"]}')
ON CONFLICT DO NOTHING;

-- Create a simple health check function
CREATE OR REPLACE FUNCTION health_check() RETURNS TABLE(
    check_name TEXT,
    status TEXT,
    details TEXT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        'database_connection'::TEXT as check_name,
        'healthy'::TEXT as status,
        'PostgreSQL connection is working'::TEXT as details
    UNION ALL
    SELECT 
        'extensions'::TEXT as check_name,
        CASE 
            WHEN EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'uuid-ossp') 
            THEN 'healthy'::TEXT
            ELSE 'unhealthy'::TEXT
        END as status,
        'Required extensions status'::TEXT as details
    UNION ALL
    SELECT 
        'schemas'::TEXT as check_name,
        CASE 
            WHEN EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = 'monitoring') 
            THEN 'healthy'::TEXT
            ELSE 'unhealthy'::TEXT
        END as status,
        'Custom schemas status'::TEXT as details;
END;
$$ LANGUAGE plpgsql;

-- Log successful initialization
INSERT INTO monitoring.system_metrics (metric_name, metric_value, metric_unit, tags) VALUES
('initialization_completed', 1, 'boolean', '{"timestamp": "' || CURRENT_TIMESTAMP || '"}');

-- Display completion message
DO $$
BEGIN
    RAISE NOTICE 'Django DBChat PostgreSQL initialization completed successfully!';
    RAISE NOTICE 'Created schemas: monitoring, analytics, staging';
    RAISE NOTICE 'Created extensions: uuid-ossp, pg_stat_statements, pg_trgm';
    RAISE NOTICE 'Created monitoring and analytics tables with indexes';
    RAISE NOTICE 'Created utility functions and views';
END $$; 