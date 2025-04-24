"""
SQL Chatbot implementation with linear flow.
"""
from typing import Any, Dict, List, Optional, Tuple
import re
from langchain_community.utilities.sql_database import SQLDatabase
from langchain_community.llms import Ollama
from sqlalchemy import text

class SQLChatbot:
    def __init__(self, db: SQLDatabase, llm: Ollama):
        """Initialize the chatbot with a database connection and LLM."""
        self.db = db
        self.llm = llm
        self.engine = db._engine
        # Table aliases for more natural language matching
        self.table_aliases = {
            'Prj_Data_Transfers_SC': ['transfer', 'transfers', 'data transfer', 'data transfers', 'recibados'],
            'greg': ['greg', 'greg table']
        }
        # Build reverse lookup for quick alias resolution
        self.alias_to_table = {}
        for table, aliases in self.table_aliases.items():
            for alias in aliases:
                self.alias_to_table[alias.lower()] = table

    def get_table_names(self) -> List[str]:
        """Get list of tables from database."""
        with self.engine.connect() as conn:
            result = conn.execute(text("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'"))
            return [row[0] for row in result]

    def extract_table_name(self, question: str) -> str:
        """Extract table name from the question using aliases and fuzzy matching."""
        tables = self.get_table_names()
        question_lower = question.lower()
        
        # First try exact table name matches
        for table in tables:
            if table.lower() in question_lower:
                return table
        
        # Then try aliases
        words = re.findall(r'\b\w+\b', question_lower)
        for i in range(len(words)):
            # Try two-word combinations first
            if i < len(words) - 1:
                two_words = words[i] + ' ' + words[i + 1]
                if two_words in self.alias_to_table:
                    return self.alias_to_table[two_words]
            
            # Try single words
            if words[i] in self.alias_to_table:
                return self.alias_to_table[words[i]]
        
        # If no match found, show available tables and aliases
        alias_help = []
        for table in tables:
            if table in self.table_aliases:
                alias_help.append(f"{table} (also known as: {', '.join(self.table_aliases[table])})")
            else:
                alias_help.append(table)
                
        raise ValueError(f"Could not find a table matching your question. Available tables:\n" + "\n".join(alias_help))

    def get_schema(self, table_name: str) -> str:
        """Get schema information for the table."""
        tables = self.get_table_names()
        if table_name not in tables:
            raise ValueError(f"Table '{table_name}' not found in database. Available tables: {', '.join(sorted(tables))}")
        
        # Get schema information directly
        with self.engine.connect() as conn:
            # Get detailed column information
            result = conn.execute(text(f"""
                SELECT 
                    COLUMN_NAME,
                    DATA_TYPE,
                    CHARACTER_MAXIMUM_LENGTH,
                    COLUMN_DEFAULT,
                    IS_NULLABLE
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_NAME = '{table_name}'
                ORDER BY ORDINAL_POSITION
            """))
            
            # Format column info more clearly
            columns = []
            for row in result:
                col_info = f"{row[0]} ({row[1]}"
                if row[2]:  # if has length
                    col_info += f"({row[2]})"
                col_info += ")"
                if row[3]:  # if has default
                    col_info += f" DEFAULT {row[3]}"
                if row[4] == 'YES':  # if nullable
                    col_info += " NULL"
                else:
                    col_info += " NOT NULL"
                columns.append(col_info)
            
            # Get sample data with column names
            result = conn.execute(text(f"SELECT TOP 3 * FROM {table_name}"))
            rows = result.fetchall()
            col_names = [col[0] for col in result.keys()]
            
            schema = f"""Table: {table_name}

Columns (name, type, constraints):
""" + "\n".join(f"- {col}" for col in columns)

            if rows:
                schema += "\n\nSample data (showing 3 rows):\n"
                for row in rows:
                    row_dict = dict(zip(col_names, row))
                    formatted_row = {k: str(v) if v is not None else 'NULL' for k, v in row_dict.items()}
                    schema += str(formatted_row) + "\n"
            
            return schema

    def generate_sql(self, question: str, schema: str) -> str:
        """Generate SQL based on question and schema."""
        prompt = f"""You are an expert in Microsoft SQL Server.
Given this database schema:
{schema}

For the Prj_Data_Transfers_SC table:
- CompanyId: The ID of the company (integer)
- Recibido: 'Y' when a transfer is received, 'N' when not received
- Despachado: 'Y' when a transfer is dispatched, 'N' when not dispatched
- Estatus: Shows the current status (e.g., 'O' for open)
- Draft_Numero: The transfer number
- Fecha_Originacion: When the transfer was created
- Fecha_Despacho: When the transfer was dispatched
- Fecha_Recibo: When the transfer was received

Generate a SQL query to answer this question: {question}

Rules:
1. Return ONLY the SQL query, no explanations
2. Use proper MS SQL Server syntax:
   - Use TOP instead of LIMIT
   - Use GETDATE() instead of NOW()
   - Use DATEADD/DATEDIFF for date math
3. Use proper case for SQL keywords (SELECT, FROM, etc.)
4. Use EXACT column names as shown in the schema (e.g., use 'CompanyId', not 'company_id')
5. Do not use backticks (`) around names
6. The query should be complete and runnable

Example outputs:
SELECT COUNT(*) FROM Prj_Data_Transfers_SC WHERE CompanyId = 1

SELECT TOP 1 * FROM Prj_Data_Transfers_SC 
WHERE Recibido = 'Y' 
ORDER BY Fecha_Recibo DESC
"""
        return self.llm.invoke(prompt).strip()

    def execute_query(self, sql: str) -> List[Tuple]:
        """Execute a SQL query and return the results."""
        try:
            # Create a new connection for each query
            with self.engine.connect() as conn:
                result = conn.execute(text(sql))
                return result.fetchall()
        except Exception as e:
            raise Exception(f"Error executing query: {str(e)}")

    def format_response(self, question: str, result: List[Tuple], sql: str) -> str:
        """Format the result in natural language."""
        prompt = f"""Given:
- Question: {question}
- SQL query used: {sql}
- Query result: {result}

Provide a clear, natural language response that:
1. Directly answers the question
2. Is concise and to the point
3. Includes relevant numbers/counts
4. Uses proper grammar and complete sentences
5. Does not include the SQL query or technical details

Example:
Question: "How many active users are there?"
Result: [(42,)]
Response: "There are 42 active users."
"""
        return self.llm.invoke(prompt).strip()

    def answer_question(self, question: str) -> str:
        """Process a question and return a natural language answer."""
        try:
            # 1. Extract table name from question
            table_name = self.extract_table_name(question)
            
            # 2. Get schema
            schema = self.get_schema(table_name)
            
            # 3. Generate SQL based on schema and question
            sql = self.generate_sql(question, schema)
            print(f"\nExecuting SQL query:\n{sql}\n")
            
            # 4. Execute SQL
            result = self.execute_query(sql)
            
            # 5. Format response
            response = self.format_response(question, result, sql)
            
            return response
            
        except Exception as e:
            return f"I apologize, but I encountered an error: {str(e)}"
