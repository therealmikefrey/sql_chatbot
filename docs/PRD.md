# WindSurf AI – PRD: Migrate from Snowflake SQLAlchemy + OpenAI to MSSQL + Local Ollama

## Objective

Refactor the existing project, which currently uses:

- **OpenAI (cloud)** for LLM interactions
- **Snowflake** via `snowflake-sqlalchemy` for database operations

To instead work with:

- **Local Ollama** (OpenAI-compatible API server)
- **Local Microsoft SQL Server** (running in Docker, accessed via `pyodbc` + `sqlalchemy`)

All other project structure and logic should remain intact.

---

## Project Context

- **Directory**: `/Users/mfrey/VSCProjects/sqlchat2/sql_chatbot`
- **Virtual Env**: `.venv` created and activated
- **Platform**: macOS
- **Dependencies**: Already installed (`pyodbc`, `sqlalchemy`, etc.)

---

## Required Changes

### 1. Database: Replace Snowflake SQLAlchemy with MSSQL SQLAlchemy

#### 2. LLM: Replace OpenAI with Local Ollama

##### SQL Settings are:
MSSQL_SERVER=localhost
MSSQL_DATABASE=testdb
MSSQL_USER=sqlchat_user
MSSQL_PASSWORD=StrongP@ssw0rd!

# SQL Server ODBC Driver
MSSQL_DRIVER=ODBC Driver 17 for SQL Server

# Ollama Settings
OLLAMA_URL=http://localhost:11434
model:  codellama:13b