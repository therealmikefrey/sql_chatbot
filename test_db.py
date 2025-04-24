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
    
    # Test query on Prj_Data_Transfers_SC
    if 'Prj_Data_Transfers_SC' in tables:
        result = conn.execute(text("SELECT COUNT(*) FROM Prj_Data_Transfers_SC WHERE Recibido = 'Y'"))
        count = result.scalar()
        print("Count:", count)
