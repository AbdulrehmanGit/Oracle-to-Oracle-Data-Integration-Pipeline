#!/usr/bin/env python3
"""
Daily CDC: Oracle -> Oracle (raw replica) for many tables.
- Staging per table: TARGET.STG_<TABLE>
- Watermark table: TARGET.ETL_WATERMARKS (created if missing)
- Watermarking uses CREATED_AT and UPDATED_AT
- CDC operations: INSERT/UPDATE (IS_DELETED='N'), SOFT DELETE (IS_DELETED='Y')
- Relationship-aware load order: parents (PK) -> children (FK)
"""
import sys
import traceback
import logging
from datetime import datetime
from typing import List, Dict, Set, Tuple
from config.settings import db_config, spark_config
from utils.database import get_connection, list_tables, get_pk_columns, get_all_columns, get_fk_relationships

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(f'cdc_etl_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
    ]
)
logger = logging.getLogger(__name__)

# Try to import Spark dependencies
try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, greatest, coalesce, lit
    SPARK_AVAILABLE = True
except ImportError:
    logger.warning("PySpark not available. Spark functionality will be disabled.")
    SPARK_AVAILABLE = False

def jdbc_url():
    """Create JDBC URL for Spark."""
    return f"jdbc:oracle:thin:@//{db_config.host}:{db_config.port}/{db_config.service_name}"

def build_spark():
    """Build Spark session."""
    if not SPARK_AVAILABLE:
        raise ImportError("PySpark is not available")
    
    return (
        SparkSession.builder
        .appName("Oracle_CDC_ETL")
        .master("local[*]")
        .config("spark.driver.extraClassPath", spark_config.ojdbc_jar)
        .config("spark.executor.extraClassPath", spark_config.ojdbc_jar)
        .getOrCreate()
    )

def ensure_watermark_table(conn):
    """Create watermark table if it doesn't exist."""
    ddl = f"""
    BEGIN
      EXECUTE IMMEDIATE '
        CREATE TABLE "{db_config.target_user}"."ETL_WATERMARKS" (
          TABLE_NAME VARCHAR2(128) PRIMARY KEY,
          LAST_TS    TIMESTAMP(6)
        )';
    EXCEPTION
      WHEN OTHERS THEN
        IF SQLCODE != -955 THEN
          RAISE;
        END IF;
    END;
    """
    with conn.cursor() as c:
        c.execute(ddl)
    conn.commit()
    logger.info("Watermark table ensured")

def get_last_watermark(conn, table: str):
    """Get last watermark for a table."""
    sql = f'SELECT LAST_TS FROM "{db_config.target_user}"."ETL_WATERMARKS" WHERE TABLE_NAME = :t'
    with conn.cursor() as c:
        c.execute(sql, t=table.upper())
        row = c.fetchone()
        return row[0] if row else None

def upsert_watermark(conn, table: str, ts_val):
    """Update or insert watermark for a table."""
    with conn.cursor() as c:
        c.execute(f"""
        MERGE INTO "{db_config.target_user}"."ETL_WATERMARKS" tgt
        USING (SELECT :t AS TABLE_NAME, :ts AS LAST_TS FROM dual) src
        ON (tgt.TABLE_NAME = src.TABLE_NAME)
        WHEN MATCHED THEN UPDATE SET tgt.LAST_TS = src.LAST_TS
        WHEN NOT MATCHED THEN INSERT (TABLE_NAME, LAST_TS) VALUES (src.TABLE_NAME, src.LAST_TS)
        """, t=table.upper(), ts=ts_val)
    conn.commit()
    logger.info(f"Watermark for {table} updated to {ts_val}")

def ensure_staging_table(conn, table: str):
    """Create staging table if it doesn't exist and truncate it."""
    tgt_table = f'"{db_config.target_user}"."{table.upper()}"'
    stg_table = f'"{db_config.target_user}"."STG_{table.upper()}"'
    
    plsql = f"""
    DECLARE
      v_count INTEGER;
    BEGIN
      SELECT COUNT(*) INTO v_count
      FROM all_tables
      WHERE owner = :own AND table_name = :tab;
      IF v_count = 0 THEN
        EXECUTE IMMEDIATE 'CREATE TABLE {stg_table} AS SELECT * FROM {tgt_table} WHERE 1=2';
      END IF;
    END;
    """
    
    with conn.cursor() as c:
        c.execute(plsql, own=db_config.target_user.upper(), tab=f"STG_{table.upper()}")
        c.execute(f"TRUNCATE TABLE {stg_table}")
    conn.commit()
    logger.info(f"Staging table for {table} ensured")

def count_table_rows(conn, schema: str, table: str) -> int:
    """Count rows in a table."""
    sql = f'SELECT COUNT(*) FROM "{schema}"."{table}"'
    with conn.cursor() as c:
        c.execute(sql)
        return c.fetchone()[0]

def run_merge_soft_delete(conn, table: str, pk_cols: List[str], all_cols: List[str]) -> Tuple[int, int]:
    """Perform CDC merge with soft delete."""
    tgt = f'"{db_config.target_user}"."{table.upper()}"'
    stg = f'"{db_config.target_user}"."STG_{table.upper()}"'

    on_clause = " AND ".join([f't.{c} = s.{c}' for c in pk_cols])
    non_pk_cols = [c for c in all_cols if c not in set(pk_cols)]
    set_list = ", ".join([f't.{c} = s.{c}' for c in non_pk_cols if c.upper() != 'IS_DELETED'])

    cols_csv = ", ".join(all_cols)
    vals_csv = ", ".join([f's.{c}' for c in all_cols])

    # Get counts before merge
    target_count_before = count_table_rows(conn, db_config.target_user, table)
    
    # Execute merge
    merge_sql = f"""
    MERGE INTO {tgt} t
    USING (SELECT * FROM {stg}) s
      ON ({on_clause})
    WHEN MATCHED THEN
      UPDATE SET
        {set_list},
        t.IS_DELETED = s.IS_DELETED
    WHEN NOT MATCHED THEN
      INSERT ({cols_csv})
      VALUES ({vals_csv})
      WHERE s.IS_DELETED = 'N'
    """
    
    with conn.cursor() as c:
        c.execute(merge_sql)
    conn.commit()
    
    # Get counts after merge
    target_count_after = count_table_rows(conn, db_config.target_user, table)
    staging_count = count_table_rows(conn, db_config.target_user, f"STG_{table}")
    
    # Calculate inserted and updated counts
    inserted_count = target_count_after - target_count_before
    updated_count = staging_count - inserted_count
    
    logger.info(f"Merge completed for {table}: {inserted_count} inserted, {updated_count} updated")
    return inserted_count, updated_count

def topo_sort_tables(tables: List[str], edges: List[Tuple[str, str]]) -> Tuple[List[str], List[str]]:
    """Topological sort of tables based on FK relationships."""
    tables_up = [t.upper() for t in tables]
    nodes = set(tables_up)
    out_adj = {n: set() for n in nodes}
    in_deg = {n: 0 for n in nodes}

    for parent, child in edges:
        if parent in nodes and child in nodes:
            if child not in out_adj[parent]:
                out_adj[parent].add(child)
                in_deg[child] += 1

    # Queue roots (no incoming edges)
    roots = [n for n in nodes if in_deg[n] == 0]
    result = []
    while roots:
        n = roots.pop()
        result.append(n)
        for m in list(out_adj[n]):
            in_deg[m] -= 1
            out_adj[n].remove(m)
            if in_deg[m] == 0:
                roots.append(m)

    leftovers = [n for n in nodes if in_deg[n] > 0]  # cycles or unresolved
    logger.info(f"Topological sort: {len(result)} tables ordered, {len(leftovers)} leftovers")
    return result, leftovers

def main():
    """Main CDC ETL function."""
    logger.info("Starting CDC ETL process")
    
    # Initialize statistics
    stats = {
        'tables_processed': 0,
        'tables_skipped': 0,
        'tables_failed': 0,
        'total_rows_processed': 0,
        'total_rows_inserted': 0,
        'total_rows_updated': 0
    }
    
    table_stats = {}
    failures = []
    
    try:
        if not SPARK_AVAILABLE:
            raise ImportError("PySpark is required for CDC ETL")
        
        spark = build_spark()
        url = jdbc_url()

        source_props = {
            "user": db_config.source_user,
            "password": db_config.source_password,
            "driver": "oracle.jdbc.OracleDriver",
            "fetchsize": str(spark_config.read_fetchsize),
        }
        
        target_props = {
            "user": db_config.target_user,
            "password": db_config.target_password,
            "driver": "oracle.jdbc.OracleDriver",
        }

        with get_connection(db_config.target_user, db_config.target_password) as tgt_conn, \
             get_connection(db_config.source_user, db_config.source_password) as src_conn:
            
            ensure_watermark_table(tgt_conn)

            # Discover tables in source
            all_tables = list_tables(src_conn, db_config.source_user)
            table_set = set(t.upper() for t in all_tables)
            logger.info(f"Found {len(all_tables)} source tables")

            # Discover FK relationships and compute load order
            edges = get_fk_relationships(src_conn, db_config.source_user, all_tables)
            ordered, leftovers = topo_sort_tables(all_tables, edges)
            
            if leftovers:
                logger.warning(f"Cycles or unresolved dependencies: {leftovers}")
                ordered += [t for t in leftovers if t not in ordered]

            # Process tables in dependency order
            for table in ordered:
                try:
                    logger.info(f"Processing table: {table}")

                    # Check for PK and CDC columns
                    pk_cols = get_pk_columns(src_conn, db_config.source_user, table)
                    if not pk_cols:
                        logger.warning(f"{table}: No primary key, skipping CDC")
                        stats['tables_skipped'] += 1
                        continue

                    all_cols = get_all_columns(src_conn, db_config.source_user, table)
                    req_cols = ("CREATED_AT", "UPDATED_AT", "IS_DELETED")
                    for r in req_cols:
                        if r not in [c.upper() for c in all_cols]:
                            raise RuntimeError(f"Missing CDC column {r}")

                    # Get watermark
                    last_ts = get_last_watermark(tgt_conn, table)
                    logger.info(f"{table}: last watermark = {last_ts}")

                    # Read delta from source
                    src_fq = f"{db_config.source_user}.{table}"
                    df = spark.read.jdbc(url=url, table=src_fq, properties=source_props)

                    change_ts = greatest(
                        coalesce(col("UPDATED_AT"), col("CREATED_AT")), 
                        col("CREATED_AT")
                    ).alias("__CHANGE_TS__")
                    
                    df = df.withColumn("__CHANGE_TS__", change_ts)

                    if last_ts is not None:
                        df_delta = df.filter(col("__CHANGE_TS__") > lit(last_ts))
                    else:
                        df_delta = df  # initial full load

                    delta_count = df_delta.count()
                    if delta_count == 0:
                        logger.info(f"{table}: no changes since {last_ts}")
                        stats['tables_processed'] += 1
                        table_stats[table] = {'rows_processed': 0, 'rows_inserted': 0, 'rows_updated': 0}
                        continue

                    logger.info(f"{table}: {delta_count} rows to apply")

                    # Stage data
                    ensure_staging_table(tgt_conn, table)
                    stg_fq = f"{db_config.target_user}.STG_{table}"

                    (df_delta.drop("__CHANGE_TS__")
                     .write
                     .format("jdbc")
                     .option("url", url)
                     .option("dbtable", stg_fq)
                     .option("user", target_props["user"])
                     .option("password", target_props["password"])
                     .option("driver", target_props["driver"])
                     .option("batchsize", spark_config.write_batchsize)
                     .mode("append")
                     .save())

                    # Merge data
                    inserted_count, updated_count = run_merge_soft_delete(tgt_conn, table, pk_cols, all_cols)

                    # Advance watermark
                    max_ts_row = df_delta.selectExpr(
                        "MAX(GREATEST(COALESCE(UPDATED_AT, CREATED_AT), CREATED_AT)) as MAX_TS"
                    ).collect()[0]
                    
                    new_ts = max_ts_row["MAX_TS"]
                    if new_ts is None:
                        logger.warning(f"{table}: computed new watermark is NULL; skipping update")
                    else:
                        upsert_watermark(tgt_conn, table, new_ts)
                        logger.info(f"{table}: applied {delta_count} changes, watermark -> {new_ts}")

                    # Update statistics
                    stats['tables_processed'] += 1
                    stats['total_rows_processed'] += delta_count
                    stats['total_rows_inserted'] += inserted_count
                    stats['total_rows_updated'] += updated_count
                    
                    table_stats[table] = {
                        'rows_processed': delta_count,
                        'rows_inserted': inserted_count,
                        'rows_updated': updated_count
                    }

                except Exception as e:
                    logger.error(f"Failed to process table {table}: {str(e)}")
                    logger.error(traceback.format_exc())
                    failures.append((table, str(e)))
                    stats['tables_failed'] += 1

        spark.stop()

        # Print summary
        logger.info("\n" + "="*60)
        logger.info("CDC ETL SUMMARY")
        logger.info("="*60)
        logger.info(f"Tables processed successfully: {stats['tables_processed']}")
        logger.info(f"Tables skipped: {stats['tables_skipped']}")
        logger.info(f"Tables failed: {stats['tables_failed']}")
        logger.info(f"Total rows processed: {stats['total_rows_processed']}")
        logger.info(f"Total rows inserted: {stats['total_rows_inserted']}")
        logger.info(f"Total rows updated: {stats['total_rows_updated']}")
        
        logger.info("\nTable-level statistics:")
        for table, t_stats in table_stats.items():
            logger.info(f"  {table}: {t_stats['rows_processed']} processed, "
                       f"{t_stats['rows_inserted']} inserted, "
                       f"{t_stats['rows_updated']} updated")

        if failures:
            logger.error("\nCDC process completed with errors:")
            for t, err in failures:
                logger.error(f" - {t}: {err}")
            sys.exit(2)
        else:
            logger.info("\nCDC process completed successfully")

    except Exception as e:
        logger.error(f"Fatal error in CDC process: {str(e)}")
        logger.error(traceback.format_exc())
        sys.exit(1)

if __name__ == "__main__":
    main()