#!/usr/bin/env python3
"""
Clone table structure from SOURCE to TARGET schema.
Generates DDL for all tables in SOURCE and applies them to TARGET schema.
Safe to re-run; it will skip tables that already exist in TARGET.
"""
import sys
import re
import logging
from config.settings import db_config
from utils.database import get_connection, list_tables, table_exists, get_table_ddl

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

def modify_ddl_for_target(ddl: str, source_user: str, target_user: str) -> str:
    """Modify DDL to work with target schema."""
    # Replace source schema with target schema
    modified_ddl = re.sub(
        f'"{source_user.upper()}"\."', 
        f'"{target_user.upper()}"."', 
        ddl, 
        flags=re.IGNORECASE
    )
    
    # Remove storage clauses to use target defaults
    modified_ddl = re.sub(r'(\s)TABLESPACE\s+"?\w+"?', r'\1', modified_ddl, flags=re.IGNORECASE)
    modified_ddl = re.sub(r'(\s)STORAGE\s*\([^)]+\)', r'\1', modified_ddl, flags=re.IGNORECASE)
    
    # Remove other storage parameters
    storage_params = ['PCTFREE', 'PCTUSED', 'INITRANS', 'MAXTRANS', 'COMPRESS', 'LOGGING']
    for param in storage_params:
        modified_ddl = re.sub(rf'(\s){param}\s+\w+', r'\1', modified_ddl, flags=re.IGNORECASE)
    
    return modified_ddl

def create_table_in_target(conn, ddl: str, table: str):
    """Create table in target schema."""
    try:
        with conn.cursor() as c:
            c.execute(ddl)
        conn.commit()
        logger.info(f"{table}: Table created successfully")
        return True
    except Exception as e:
        logger.error(f"{table}: Failed to create table - {e}")
        conn.rollback()
        return False

def main():
    """Main function to clone schema structure."""
    try:
        # Connect to both source and target
        with get_connection(db_config.source_user, db_config.source_password) as src_conn, \
             get_connection(db_config.target_user, db_config.target_password) as tgt_conn:
            
            # Get list of tables from source
            tables = list_tables(src_conn, db_config.source_user)
            logger.info(f"Found {len(tables)} tables in {db_config.source_user}: {tables}")
            
            created_count = 0
            skipped_count = 0
            failed_count = 0
            
            for table in tables:
                # Check if table already exists in target
                if table_exists(tgt_conn, db_config.target_user, table):
                    logger.info(f"{table}: Already exists in target")
                    skipped_count += 1
                    continue
                
                # Get DDL from source
                ddl = get_table_ddl(src_conn, db_config.source_user, table)
                if not ddl:
                    logger.error(f"{table}: Could not retrieve DDL")
                    failed_count += 1
                    continue
                
                # Modify DDL for target schema
                modified_ddl = modify_ddl_for_target(ddl, db_config.source_user, db_config.target_user)
                
                # Create table in target
                if create_table_in_target(tgt_conn, modified_ddl, table):
                    created_count += 1
                else:
                    failed_count += 1
            
            logger.info(f"Schema cloning complete:")
            logger.info(f"  Created: {created_count} tables")
            logger.info(f"  Skipped: {skipped_count} tables")
            logger.info(f"  Failed: {failed_count} tables")
            
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()