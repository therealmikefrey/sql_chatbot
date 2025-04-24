from sqlalchemy import create_engine, URL, text
from sql_analyzer.config import cfg

# Create connection URL
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

# Create engine and test connection
engine = create_engine(connection_url)
with engine.connect() as conn:
    # Get table names directly
    result = conn.execute(text("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'"))
    tables = [row[0] for row in result]
    print("Tables:", tables)
    
    # Show schema for Prj_Data_Transfers_SC
    result = conn.execute(text("""
        SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = 'Prj_Data_Transfers_SC'
        ORDER BY ORDINAL_POSITION
    """))
    print("\nColumns in Prj_Data_Transfers_SC:")
    for row in result:
        print(f"- {row[0]} ({row[1]})")
    
    # Show sample data
    result = conn.execute(text("SELECT TOP 1 * FROM Prj_Data_Transfers_SC"))
    row = result.fetchone()
    if row:
        print("\nSample row:")
        for col, val in zip(result.keys(), row):
            print(f"{col}: {val}")
