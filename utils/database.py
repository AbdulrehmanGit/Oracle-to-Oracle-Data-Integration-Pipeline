"""
Database utility functions for Oracle to Oracle ETL.
"""
import oracledb
from typing import List, Dict, Any
from config.settings import db_config

def get_dsn():
    """Create DSN for Oracle connection."""
    return oracledb.makedsn(db_config.host, db_config.port, service_name=db_config.service_name)

def get_connection(user: str, password: str):
    """Create a database connection."""
    dsn = get_dsn()
    return oracledb.connect(user=user, password=password, dsn=dsn)

def list_tables(conn, owner: str, exclude_temporary: bool = True) -> List[str]:
    """List all tables for a given owner."""
    sql = """
      SELECT table_name
      FROM all_tables
      WHERE owner = :owner
    """
    if exclude_temporary:
        sql += " AND temporary = 'N'"
    sql += " AND table_name NOT LIKE 'BIN$%' ORDER BY table_name"
    
    with conn.cursor() as c:
        c.execute(sql, owner=owner.upper())
        return [r[0] for r in c.fetchall()]

def column_exists(conn, owner: str, table: str, col: str) -> bool:
    """Check if a column exists in a table."""
    sql = """
      SELECT 1 FROM all_tab_columns
      WHERE owner=:own AND table_name=:tab AND column_name=:col
    """
    with conn.cursor() as c:
        c.execute(sql, own=owner.upper(), tab=table.upper(), col=col.upper())
        return c.fetchone() is not None

def table_exists(conn, owner: str, table: str) -> bool:
    """Check if a table exists."""
    sql = "SELECT 1 FROM all_tables WHERE owner=:own AND table_name=:tab"
    with conn.cursor() as c:
        c.execute(sql, own=owner.upper(), tab=table.upper())
        return c.fetchone() is not None

def get_table_ddl(conn, owner: str, table: str) -> str:
    """Get DDL for a table."""
    sql = "SELECT DBMS_METADATA.GET_DDL('TABLE', :table_name, :owner) FROM dual"
    with conn.cursor() as c:
        c.execute(sql, table_name=table.upper(), owner=owner.upper())
        result = c.fetchone()
        return result[0] if result else None

def get_pk_columns(conn, owner: str, table: str) -> List[str]:
    """Get primary key columns for a table."""
    sql = """
    SELECT col.column_name
    FROM all_constraints con
    JOIN all_cons_columns col
      ON con.owner = col.owner
     AND con.constraint_name = col.constraint_name
    WHERE con.owner = :own
      AND con.table_name = :tab
      AND con.constraint_type = 'P'
    ORDER BY col.position
    """
    with conn.cursor() as c:
        c.execute(sql, own=owner.upper(), tab=table.upper())
        return [r[0] for r in c.fetchall()]

def get_all_columns(conn, owner: str, table: str) -> List[str]:
    """Get all columns for a table."""
    sql = """
    SELECT column_name
    FROM all_tab_columns
    WHERE owner=:own AND table_name=:tab
    ORDER BY column_id
    """
    with conn.cursor() as c:
        c.execute(sql, own=owner.upper(), tab=table.upper())
        return [r[0] for r in c.fetchall()]

def get_fk_relationships(conn, owner: str, tables: List[str]) -> List[tuple]:
    """Get foreign key relationships between tables."""
    table_set = set(t.upper() for t in tables)
    sql = """
    SELECT
      c_fk.table_name AS child_table,
      c_pk.table_name AS parent_table
    FROM all_constraints c_fk
    JOIN all_constraints c_pk
      ON c_fk.r_owner = c_pk.owner
     AND c_fk.r_constraint_name = c_pk.constraint_name
    WHERE c_fk.constraint_type = 'R'
      AND c_fk.owner = :own
      AND c_pk.constraint_type = 'P'
    """
    edges = []
    with conn.cursor() as cur:
        cur.execute(sql, own=owner.upper())
        for child, parent in cur.fetchall():
            child = child.upper()
            parent = parent.upper()
            if parent in table_set and child in table_set:
                edges.append((parent, child))
    return edges
