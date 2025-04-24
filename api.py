from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from sql_analyzer.agent_factory import init_chatbot

app = FastAPI()
chatbot = init_chatbot()

class Question(BaseModel):
    text: str

@app.get("/tables")
async def get_tables():
    """Get available tables."""
    try:
        tables = chatbot.get_table_names()
        return {"tables": tables}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/query")
async def process_query(question: Question):
    """Process a natural language query."""
    try:
        # Extract table name from question
        table_name = chatbot.extract_table_name(question.text)
        
        # Get schema
        schema = chatbot.get_schema(table_name)
        
        # Generate SQL
        sql = chatbot.generate_sql(question.text, schema)
        
        # Execute query
        result = chatbot.execute_query(sql)
        
        # Format response
        response = chatbot.format_response(question.text, result, sql)
        
        return {
            "sql": sql,
            "response": response
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
