from langchain_community.utilities.sql_database import SQLDatabase
from sqlalchemy import create_engine, text
from sqlalchemy.pool import SingletonThreadPool

from sql_analyzer.config import MSSQL, MYSQL, cfg
from sql_analyzer.log_init import logger


def sql_db_factory() -> SQLDatabase:
    if cfg.selected_db == MSSQL:
        mssql_config = cfg.mssql_config
        
        # Create connection string for pyodbc
        conn_str = (
            "DRIVER={" + mssql_config.driver + "};"
            "SERVER=" + mssql_config.server + ";"
            "DATABASE=" + mssql_config.database + ";"
            "UID=" + mssql_config.user + ";"
            "PWD=" + mssql_config.password + ";"
            "TrustServerCertificate=yes;"
        )
        
        # Create engine with singleton pool
        engine = create_engine(
            f"mssql+pyodbc:///?odbc_connect={conn_str}",
            poolclass=SingletonThreadPool,  # Use a single connection
            connect_args={
                "autocommit": True
            }
        )
        
        # Create SQLDatabase instance
        sql_db = SQLDatabase(
            engine=engine,
            schema="dbo",  # Default schema for MSSQL
            sample_rows_in_table_info=3
        )
        
        # Test the connection and log table names
        tables = sql_db.get_usable_table_names()
        logger.info("Available tables: %s", tables)
        
        return sql_db
    elif cfg.selected_db == MYSQL:
        return SQLDatabase.from_uri(cfg.db_uri, view_support=True)
    else:
        raise Exception(f"Could not create sql database factory: {cfg.selected_db}")


if __name__ == "__main__":
    logger.info("sql_db_factory")
    sql_database = sql_db_factory()
    logger.info("sql_database %s", sql_database)
