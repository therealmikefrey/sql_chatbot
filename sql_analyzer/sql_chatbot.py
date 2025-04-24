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

    def get_table_names(self) -> List[str]:
        """Get list of tables from database."""
        with self.engine.connect() as conn:
            result = conn.execute(text("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'"))
            return [row[0] for row in result]

    def extract_table_name(self, question: str) -> str:
        """Extract table name from the question using simple regex."""
        tables = self.get_table_names()
        
        # First try exact matches
        for table in tables:
            if table in question:
                return table
        
        # Then try case-insensitive
        for table in tables:
            if table.lower() in question.lower():
                return table
        
        raise ValueError(f"Could not find a valid table name in the question. Available tables: {', '.join(sorted(tables))}")

    def get_schema(self, table_name: str) -> str:
        """Get schema information for the table."""
        tables = self.get_table_names()
        if table_name not in tables:
            raise ValueError(f"Table '{table_name}' not found in database. Available tables: {', '.join(sorted(tables))}")
        
        # Get schema information directly
        with self.engine.connect() as conn:
            # Get column information
            result = conn.execute(text(f"""
                SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_NAME = '{table_name}'
                ORDER BY ORDINAL_POSITION
            """))
            columns = [f"{row[0]} {row[1]}" + (f"({row[2]})" if row[2] else "") for row in result]
            
            # Get sample data
            result = conn.execute(text(f"SELECT TOP 3 * FROM {table_name}"))
            rows = result.fetchall()
            columns = [col[0] for col in result.keys()]
            
            schema = f"Table: {table_name}\nColumns:\n" + "\n".join(columns)
            if rows:
                schema += "\n\nSample data:\n"
                for row in rows:
                    schema += str(dict(zip(columns, row))) + "\n"
            
            return schema

    def generate_sql(self, question: str, schema: str) -> str:
        """Generate SQL based on question and schema."""
        prompt = f"""You are an expert in Microsoft SQL Server.
Given this database schema:
{schema}

Generate a SQL query to answer this question: {question}

Rules:
1. Return ONLY the SQL query, no explanations
2. Use proper MS SQL Server syntax
3. Use proper case for SQL keywords (SELECT, FROM, etc.)
4. Do not include any markdown formatting
5. The query should be complete and runnable

Example output:
SELECT COUNT(*) FROM Users WHERE active = 1
"""
        return self.llm.invoke(prompt).strip()

    def execute_query(self, sql: str) -> List[Tuple]:
        """Execute the SQL and return results."""
        with self.engine.connect() as conn:
            result = conn.execute(text(sql))
            return result.fetchall()

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
            
            # 3 & 4. Generate SQL based on schema and question
            sql = self.generate_sql(question, schema)
            
            # 5. Execute SQL
            result = self.execute_query(sql)
            
            # 6. Format response
            response = self.format_response(question, result, sql)
            
            return response
            
        except Exception as e:
            return f"I apologize, but I encountered an error: {str(e)}"
