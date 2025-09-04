"""
Configuration settings for Oracle to Oracle ETL pipeline.
"""
import os
from dataclasses import dataclass

@dataclass
class DatabaseConfig:
    host: str = os.getenv("ORACLE_HOST", "<HOST>")
    port: int = int(os.getenv("ORACLE_PORT","<PORT>"))
    service_name: str = os.getenv("ORACLE_SERVICE_NAME", "<SERVICE NAME>")
    source_user: str = os.getenv("SOURCE_USER", "<SOURCE SCHEMA>")
    source_password: str = os.getenv("SOURCE_PASSWORD", "<PASSWORD>")
    target_user: str = os.getenv("TARGET_USER", "<TARGET SCHEMA>")
    target_password: str = os.getenv("TARGET_PASSWORD", "<PASSWORD>")

@dataclass
class CDCConfig:
    cdc_columns: list = None
    
    def __post_init__(self):
        if self.cdc_columns is None:
            self.cdc_columns = [
                ("CREATED_AT", "TIMESTAMP", "DEFAULT SYSTIMESTAMP"),
                ("UPDATED_AT", "TIMESTAMP", "NULL"),
                ("IS_DELETED", "CHAR(1)", "DEFAULT 'N' NOT NULL"),
            ]

@dataclass
class SparkConfig:
    ojdbc_jar: str = os.getenv("OJDBC_JAR", "/path/to/ojdbc8.jar")
    read_fetchsize: int = int(os.getenv("READ_FETCHSIZE", 10000))
    write_batchsize: int = int(os.getenv("WRITE_BATCHSIZE", 5000))

# Initialize configurations
db_config = DatabaseConfig()
cdc_config = CDCConfig()
spark_config = SparkConfig()
