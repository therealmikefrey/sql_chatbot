from langchain_community.utilities.sql_database import SQLDatabase
from sqlalchemy import create_engine, URL, text

from sql_analyzer.config import MSSQL, MYSQL, cfg
from sql_analyzer.log_init import logger


def sql_db_factory() -> SQLDatabase:
    if cfg.selected_db == MSSQL:
        mssql_config = cfg.mssql_config
        connection_url = URL.create(
            "mssql+pyodbc",
            username=mssql_config.user,
            password=mssql_config.password,
            host=mssql_config.server,
            database=mssql_config.database,
            query={
                "driver": mssql_config.driver,
                "TrustServerCertificate": "yes",
            },
        )
        engine = create_engine(connection_url)
        
        # Create SQLDatabase instance with engine directly
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
