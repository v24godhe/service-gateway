-- Query Learning Database Schema for SQL Server
-- Target Server: 192.168.1.42
-- Database: query_learning_db

-- Create Database
IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'query_learning_db')
BEGIN
    CREATE DATABASE query_learning_db;
END
GO

USE query_learning_db;
GO

-- Table 1: Query History
-- Stores all user queries with execution details
CREATE TABLE query_history (
    id INT IDENTITY(1,1) PRIMARY KEY,
    user_id NVARCHAR(100) NOT NULL,
    user_role NVARCHAR(50) NOT NULL,
    question NVARCHAR(MAX) NOT NULL,
    sql_generated NVARCHAR(MAX),
    execution_time_ms INT,
    success BIT NOT NULL,
    error_message NVARCHAR(MAX),
    row_count INT,
    timestamp DATETIME2 DEFAULT GETDATE(),
    session_id NVARCHAR(100),
    INDEX IX_user_timestamp (user_id, timestamp),
    INDEX IX_success (success),
    INDEX IX_role (user_role)
);
GO

-- Table 2: Query Cache
-- Caches frequently asked queries for instant responses
CREATE TABLE query_cache (
    id INT IDENTITY(1,1) PRIMARY KEY,
    query_hash NVARCHAR(64) NOT NULL UNIQUE,
    question NVARCHAR(MAX) NOT NULL,
    sql_query NVARCHAR(MAX) NOT NULL,
    result_json NVARCHAR(MAX),
    created_at DATETIME2 DEFAULT GETDATE(),
    last_accessed DATETIME2 DEFAULT GETDATE(),
    ttl_minutes INT DEFAULT 60,
    hit_count INT DEFAULT 0,
    expires_at AS DATEADD(MINUTE, ttl_minutes, last_accessed),
    INDEX IX_query_hash (query_hash),
    INDEX IX_expires (expires_at)
);
GO

-- Table 3: Query Patterns
-- Learns common query patterns by role
CREATE TABLE query_patterns (
    id INT IDENTITY(1,1) PRIMARY KEY,
    intent NVARCHAR(200) NOT NULL,
    user_role NVARCHAR(50) NOT NULL,
    pattern_template NVARCHAR(MAX),
    common_parameters NVARCHAR(MAX),
    frequency INT DEFAULT 1,
    avg_execution_time_ms INT,
    success_rate DECIMAL(5,2),
    last_used DATETIME2 DEFAULT GETDATE(),
    created_at DATETIME2 DEFAULT GETDATE(),
    INDEX IX_role_intent (user_role, intent),
    INDEX IX_frequency (frequency DESC)
);
GO

-- Table 4: Performance Metrics
-- Tracks query performance for optimization
CREATE TABLE performance_metrics (
    id INT IDENTITY(1,1) PRIMARY KEY,
    query_hash NVARCHAR(64) NOT NULL,
    avg_time_ms INT,
    min_time_ms INT,
    max_time_ms INT,
    execution_count INT DEFAULT 1,
    last_execution DATETIME2 DEFAULT GETDATE(),
    optimization_status NVARCHAR(50) DEFAULT 'normal', -- normal, slow, optimized
    needs_review BIT DEFAULT 0,
    INDEX IX_query_hash (query_hash),
    INDEX IX_optimization (optimization_status)
);
GO

-- Table 5: Cache Hit Statistics
-- Tracks cache efficiency
CREATE TABLE cache_statistics (
    id INT IDENTITY(1,1) PRIMARY KEY,
    date DATE DEFAULT CAST(GETDATE() AS DATE),
    total_queries INT DEFAULT 0,
    cache_hits INT DEFAULT 0,
    cache_misses INT DEFAULT 0,
    hit_rate AS CAST(cache_hits AS DECIMAL(5,2)) / NULLIF(total_queries, 0) * 100,
    avg_response_time_ms INT,
    UNIQUE (date)
);
GO

-- Stored Procedure: Log Query Execution
CREATE PROCEDURE sp_log_query
    @user_id NVARCHAR(100),
    @user_role NVARCHAR(50),
    @question NVARCHAR(MAX),
    @sql_generated NVARCHAR(MAX) = NULL,
    @execution_time_ms INT = 0,
    @success BIT,
    @error_message NVARCHAR(MAX) = NULL,
    @row_count INT = 0,
    @session_id NVARCHAR(100) = NULL
AS
BEGIN
    INSERT INTO query_history (user_id, user_role, question, sql_generated, execution_time_ms, success, error_message, row_count, session_id)
    VALUES (@user_id, @user_role, @question, @sql_generated, @execution_time_ms, @success, @error_message, @row_count, @session_id);
END
GO

-- Stored Procedure: Get Cached Query
CREATE PROCEDURE sp_get_cached_query
    @query_hash NVARCHAR(64)
AS
BEGIN
    UPDATE query_cache 
    SET hit_count = hit_count + 1, 
        last_accessed = GETDATE()
    WHERE query_hash = @query_hash 
        AND expires_at > GETDATE();
    
    SELECT question, sql_query, result_json
    FROM query_cache
    WHERE query_hash = @query_hash 
        AND expires_at > GETDATE();
END
GO

-- Stored Procedure: Save to Cache
CREATE PROCEDURE sp_save_to_cache
    @query_hash NVARCHAR(64),
    @question NVARCHAR(MAX),
    @sql_query NVARCHAR(MAX),
    @result_json NVARCHAR(MAX),
    @ttl_minutes INT = 60
AS
BEGIN
    MERGE query_cache AS target
    USING (SELECT @query_hash AS query_hash) AS source
    ON target.query_hash = source.query_hash
    WHEN MATCHED THEN
        UPDATE SET 
            result_json = @result_json,
            last_accessed = GETDATE(),
            ttl_minutes = @ttl_minutes,
            hit_count = 0
    WHEN NOT MATCHED THEN
        INSERT (query_hash, question, sql_query, result_json, ttl_minutes)
        VALUES (@query_hash, @question, @sql_query, @result_json, @ttl_minutes);
END
GO

-- Stored Procedure: Clean Expired Cache
CREATE PROCEDURE sp_clean_expired_cache
AS
BEGIN
    DELETE FROM query_cache WHERE expires_at < GETDATE();
END
GO

-- Stored Procedure: Update Performance Metrics
CREATE PROCEDURE sp_update_performance
    @query_hash NVARCHAR(64),
    @execution_time_ms INT
AS
BEGIN
    MERGE performance_metrics AS target
    USING (SELECT @query_hash AS query_hash) AS source
    ON target.query_hash = source.query_hash
    WHEN MATCHED THEN
        UPDATE SET 
            avg_time_ms = (avg_time_ms * execution_count + @execution_time_ms) / (execution_count + 1),
            min_time_ms = CASE WHEN @execution_time_ms < min_time_ms THEN @execution_time_ms ELSE min_time_ms END,
            max_time_ms = CASE WHEN @execution_time_ms > max_time_ms THEN @execution_time_ms ELSE max_time_ms END,
            execution_count = execution_count + 1,
            last_execution = GETDATE(),
            needs_review = CASE WHEN @execution_time_ms > 5000 THEN 1 ELSE needs_review END
    WHEN NOT MATCHED THEN
        INSERT (query_hash, avg_time_ms, min_time_ms, max_time_ms, execution_count)
        VALUES (@query_hash, @execution_time_ms, @execution_time_ms, @execution_time_ms, 1);
END
GO

-- Create SQL Agent Job for Cache Cleanup (Run every hour)
-- Note: This requires SQL Server Agent to be running
-- Manual alternative: Schedule via Task Scheduler or cron job
/*
USE msdb;
GO
EXEC sp_add_job @job_name = 'Clean Query Cache';
EXEC sp_add_jobstep @job_name = 'Clean Query Cache', 
    @step_name = 'Run Cleanup', 
    @command = 'EXEC query_learning_db.dbo.sp_clean_expired_cache';
EXEC sp_add_schedule @schedule_name = 'Hourly', 
    @freq_type = 4, 
    @freq_interval = 1, 
    @freq_subday_type = 8, 
    @freq_subday_interval = 1;
EXEC sp_attach_schedule @job_name = 'Clean Query Cache', @schedule_name = 'Hourly';
EXEC sp_add_jobserver @job_name = 'Clean Query Cache';
GO
*/

PRINT 'Query Learning Database Schema Created Successfully';
PRINT 'Database: query_learning_db on 192.168.1.42';
GO