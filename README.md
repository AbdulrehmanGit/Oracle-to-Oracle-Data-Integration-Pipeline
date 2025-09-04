Oracle to Oracle Data Integration Pipeline
==========================================
[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/) [![Spark 3.x](https://img.shields.io/badge/spark-3.x-orange.svg)](https://spark.apache.org/)

A comprehensive, production-ready ETL pipeline designed for efficient data replication between Oracle databases with robust Change Data Capture (CDC) capabilities. This solution enables near-real-time data synchronization while maintaining data integrity and relationship consistency.

Table of Contents
-----------------

-   [Overview](https://chat.z.ai/c/d556d1e5-dc47-4196-928f-7eeed3b87d80#overview)
-   [Architecture](https://chat.z.ai/c/d556d1e5-dc47-4196-928f-7eeed3b87d80#architecture)
-   [Key Features](https://chat.z.ai/c/d556d1e5-dc47-4196-928f-7eeed3b87d80#key-features)
-   [Prerequisites](https://chat.z.ai/c/d556d1e5-dc47-4196-928f-7eeed3b87d80#prerequisites)
-   [Installation](https://chat.z.ai/c/d556d1e5-dc47-4196-928f-7eeed3b87d80#installation)
-   [Configuration](https://chat.z.ai/c/d556d1e5-dc47-4196-928f-7eeed3b87d80#configuration)
-   [Usage](https://chat.z.ai/c/d556d1e5-dc47-4196-928f-7eeed3b87d80#usage)
-   [Operational Considerations](https://chat.z.ai/c/d556d1e5-dc47-4196-928f-7eeed3b87d80#operational-considerations)
-   [Troubleshooting](https://chat.z.ai/c/d556d1e5-dc47-4196-928f-7eeed3b87d80#troubleshooting)
-   [Contributing](https://chat.z.ai/c/d556d1e5-dc47-4196-928f-7eeed3b87d80#contributing)
-   [License](https://chat.z.ai/c/d556d1e5-dc47-4196-928f-7eeed3b87d80#license)

Overview
--------

This pipeline provides a complete solution for replicating data from a source Oracle database to a target Oracle database, addressing common challenges in data synchronization projects:

-   Automated CDC Implementation: Adds necessary tracking columns (CREATED_AT, UPDATED_AT, IS_DELETED) to source tables
-   Schema Synchronization: Automatically clones table structures from source to target
-   Incremental Data Loading: Efficiently transfers only changed data using timestamp-based watermarks
-   Dependency-Aware Processing: Handles foreign key relationships by loading parent tables before child tables
-   Soft Delete Support: Preserves deleted records by marking them rather than physically deleting
-   Comprehensive Monitoring: Detailed logging and statistics for operational visibility

This solution is ideal for scenarios such as:

-   Reporting database synchronization
-   Database migration with minimal downtime
-   Creating a real-time data warehouse
-   High availability and disaster recovery setups

Architecture
------------

The pipeline follows a modular three-stage process that ensures data consistency and integrity throughout the replication lifecycle:

```bash
┌─────────────────────┐    ┌─────────────────────┐    ┌─────────────────────┐
│   Preparation Phase │───▶│ Schema Sync Phase   │───▶│ Replication Phase  │
│                     │    │                     │    │                     │
│ • Add CDC columns   │    │ • Clone table DDL   │    │ • Incremental sync  │
│ • Set constraints   │    │ • Adapt storage     │    │ • Maintain order    │
│ • Initialize values │    │ • Create structures │    │ • Update watermarks │
└─────────────────────┘    └─────────────────────┘    └─────────────────────┘
```
### Preparation Phase

During this initial phase, the pipeline modifies source tables to include CDC tracking columns. This is a one-time setup that enables change tracking for all subsequent replication operations.

### Schema Synchronization Phase

This phase replicates table structures from the source to the target database, ensuring that the target environment matches the source schema while adapting storage parameters as needed.

### Continuous Replication Phase

The final phase handles the ongoing incremental transfer of changed data while maintaining referential integrity through dependency-aware processing.

Key Features
------------

### Automated CDC Column Preparation

-   Adds standardized CDC columns (CREATED_AT, UPDATED_AT, IS_DELETED) to all source tables
-   Safe for re-execution (only adds missing columns)
-   Maintains data integrity with proper default values and constraints
-   Automatically initializes timestamp values for existing records

### Intelligent Schema Cloning

-   Extracts complete DDL from source tables
-   Automatically adapts storage parameters for target environment
-   Preserves all constraints, indexes, and column properties
-   Skips already existing tables to avoid conflicts
-   Handles complex data types and table structures

### Efficient Change Data Capture

-   Uses timestamp-based watermarks for incremental extraction
-   Processes only changed records since last execution
-   Handles both inserts/updates and soft deletes
-   Maintains watermark metadata for reliable restart capability
-   Optimized for minimal impact on source system performance

### Relationship-Aware Processing

-   Automatically detects foreign key relationships
-   Orders table processing using topological sorting
-   Ensures parent tables are loaded before child tables
-   Handles circular dependencies gracefully
-   Maintains referential integrity throughout the process

### Operational Robustness

-   Comprehensive logging at multiple levels
-   Detailed performance statistics and metrics
-   Error handling with continuation for non-fatal errors
-   Configurable batch sizes for optimal performance
-   Automatic recovery and restart capabilities

Prerequisites
-------------

### Software Requirements

-   Python 3.8 or higher: Required for running the ETL scripts
-   Oracle Client libraries: Instant client or full installation for database connectivity
-   Java 8 or higher: Required for Apache Spark runtime
-   Apache Spark 3.x: Included via PySpark for distributed data processing

### Database Requirements

-   Access to source Oracle database with read privileges
-   Access to target Oracle database with read/write privileges
-   Sufficient tablespace in target database for replicated data
-   Appropriate permissions to modify table structures (for initial setup)

### Network Requirements

-   Network connectivity between ETL server and both databases
-   Appropriate firewall rules for database connections
-   Sufficient bandwidth for expected data volumes
-   Stable network connection for reliable replication

Installation
------------

### 1\. Clone the Repository

```bash


git clone <repository-url>

cd oracle-to-oracle-etl
```
### 2\. Install Python Dependencies

```bash



pip install -r requirements.txt
```
This will install all required Python packages including PySpark, Oracle DB drivers, and supporting libraries.

### 3\. Download Oracle JDBC Driver

1.  Download `ojdbc8.jar` from Oracle's website
2.  Place it in an appropriate directory (e.g., `lib/`)
3.  Update the configuration with the correct path

### 4\. Configure Environment Variables

```bash
# Database Connection

export ORACLE_HOST=your-oracle-host

export ORACLE_PORT=1521

export ORACLE_SERVICE_NAME=your-service-name

# Source Database Credentials

export SOURCE_USER=source_schema

export SOURCE_PASSWORD=source_password

# Target Database Credentials

export TARGET_USER=target_schema

export TARGET_PASSWORD=target_password

# Spark Configuration

export OJDBC_JAR=/path/to/ojdbc8.jar

export READ_FETCHSIZE=10000

export WRITE_BATCHSIZE=5000
```
Alternatively, edit the `config/settings.py` file directly with your configuration.

Configuration
-------------

### Database Connection Settings

# Database Connection
```BASH
export ORACLE_HOST=your-oracle-host
export ORACLE_PORT=1521
export ORACLE_SERVICE_NAME=your-service-name
```
# Source Database Credentials
```BASH
export SOURCE_USER=source_schema
export SOURCE_PASSWORD=source_password
```
# Target Database Credentials
```BASH
export TARGET_USER=target_schema
export TARGET_PASSWORD=target_password
```
# Spark Configuration
```BASH
export OJDBC_JAR=/path/to/ojdbc8.jar
export READ_FETCHSIZE=10000
export WRITE_BATCHSIZE=5000### Performance Tuning Settings
```
Usage
-----

### Step 1: Prepare CDC Columns in Source

Execute the preparation script to add CDC columns to all tables in the source schema:


python scripts/01_prepare_cdc_columns.py

This script:

-   Identifies all tables in the source schema
-   Adds missing CDC columns with appropriate data types and constraints
-   Initializes timestamp values for existing records
-   Is safe to re-run (only adds missing columns)

Note: This step requires ALTER TABLE privileges on the source schema and should be performed during a maintenance window if the source database is actively used.

### Step 2: Clone Schema Structure to Target

Replicate the table structures from source to target database:

python scripts/02_clone_schema_structure.py

This script:

-   Extracts DDL for all source tables
-   Modifies DDL for target environment (removes storage-specific clauses)
-   Creates tables in target schema
-   Skips tables that already exist in target
-   Preserves primary keys, indexes, and constraints

Note: This step only creates the table structures and does not transfer any data.

### Step 3: Execute CDC ETL Process

Perform incremental data replication:

python scripts/03_cdc_etl.py

This script:

-   Identifies changed records using timestamp watermarks
-   Processes tables in dependency order (parents before children)
-   Handles inserts, updates, and soft deletes
-   Updates watermarks for subsequent runs
-   Generates comprehensive execution reports

For ongoing replication, this step should be scheduled to run at regular intervals (e.g., every 5 minutes, hourly, etc.) depending on your data freshness requirements.

Operational Considerations
--------------------------

### Scheduling

For near-real-time replication, schedule Step 3 to run frequently. The optimal frequency depends on:

-   Source system workload and transaction volume
-   Network bandwidth between databases
-   Target database capacity
-   Business requirements for data freshness

Consider using a scheduler like cron, Airflow, or any enterprise scheduling tool to automate the execution.

### Monitoring

Regular monitoring is essential for maintaining a healthy replication pipeline:

1.  Log Files: Review log files in the `logs/` directory for operational insights
2.  Database Growth: Monitor tablespace usage in the target environment
3.  Performance Metrics: Track ETL execution times and record counts
4.  Alerting: Set up alerts for failed executions or unusual patterns

### Error Handling

The pipeline is designed to be resilient:

-   Continues processing after non-fatal errors
-   Failed table processing is logged and reported
-   Watermarks are only updated after successful processing
-   Detailed error messages facilitate troubleshooting

### Performance Optimization

To optimize performance:

-   Adjust fetch and batch sizes based on network and database performance
-   Consider partitioning large tables for parallel processing
-   Monitor and tune memory allocation for Spark operations
-   Evaluate network latency and bandwidth between databases
-   Schedule heavy loads during off-peak hours

Troubleshooting
---------------

### Common Issues

1.  Connection Errors

    -   Verify network connectivity between the ETL server and databases
    -   Check that firewall rules allow database connections
    -   Ensure credentials are correct and accounts are not locked
2.  Permission Errors

    -   Verify that database users have the required privileges
    -   Check that tablespace quotas are sufficient
    -   Ensure the schema user can create tables and indexes
3.  Performance Issues

    -   Monitor database performance during ETL execution
    -   Check for network bottlenecks
    -   Review Spark configuration settings
    -   Consider increasing batch sizes for large data volumes
4.  Data Integrity Issues

    -   Verify that CDC columns are properly maintained in the source
    -   Check for application processes that bypass CDC tracking
    -   Review dependency ordering for complex relationships


-------
