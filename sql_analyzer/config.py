import os

from dotenv import load_dotenv
from sql_analyzer.log_init import logger

load_dotenv()


SNOWFLAKE = "snowflake"
MYSQL = "mysql"
MSSQL = "mssql"
SELECTED_DBS = [MSSQL, MYSQL]


class MSSQLConfig:
    server = os.getenv("MSSQL_SERVER")
    database = os.getenv("MSSQL_DATABASE")
    user = os.getenv("MSSQL_USER")
    password = os.getenv("MSSQL_PASSWORD")
    driver = os.getenv("MSSQL_DRIVER", "ODBC Driver 17 for SQL Server")


class Config:
    # Ollama Configuration
    ollama_url = os.getenv("OLLAMA_URL", "http://localhost:11434")
    ollama_model = os.getenv("OLLAMA_MODEL", "codellama:13b")

    # Database Configuration
    db_uri = os.getenv("DB_CONNECTION_STRING")
    mssql_config = MSSQLConfig()
    selected_db = os.getenv("SELECTED_DB")
    if selected_db not in SELECTED_DBS:
        raise Exception(
            f"Selected DB {selected_db} not recognized. The possible values are: {SELECTED_DBS}."
        )


cfg = Config()

if __name__ == "__main__":
    logger.info("Ollama URL: %s", cfg.ollama_url)
    logger.info("Ollama Model: %s", cfg.ollama_model)
    logger.info("db_uri: %s", cfg.db_uri)
    logger.info("selected_db: %s", cfg.selected_db)
