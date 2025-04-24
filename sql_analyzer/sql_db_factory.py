from langchain_community.utilities.sql_database import SQLDatabase
from sqlalchemy import create_engine, URL

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
        return SQLDatabase(engine=engine)
    elif cfg.selected_db == MYSQL:
        return SQLDatabase.from_uri(cfg.db_uri, view_support=True)
    else:
        raise Exception(f"Could not create sql database factory: {cfg.selected_db}")


if __name__ == "__main__":
    logger.info("sql_db_factory")
    sql_database = sql_db_factory()
    logger.info("sql_database %s", sql_database)
